package com.upplication.s3fs;

import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.internal.Constants;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.upplication.s3fs.attribute.S3BasicFileAttributeView;
import com.upplication.s3fs.attribute.S3BasicFileAttributes;
import com.upplication.s3fs.attribute.S3PosixFileAttributeView;
import com.upplication.s3fs.attribute.S3PosixFileAttributes;
import com.upplication.s3fs.util.AttributesUtils;
import com.upplication.s3fs.util.Cache;
import com.upplication.s3fs.util.S3Utils;

import java.io.*;
import java.net.URI;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.nio.file.spi.FileSystemProvider;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.collect.Sets.difference;
import static com.upplication.s3fs.AmazonS3Factory.*;
import static java.lang.String.format;

/**
 * Spec:
 * <p>
 * URI: s3://[endpoint]/{bucket}/{key} If endpoint is missing, it's assumed to
 * be the default S3 endpoint (s3.amazonaws.com)
 * </p>
 * <p>
 * FileSystem roots: /{bucket}/
 * </p>
 * <p>
 * Treatment of S3 objects: - If a key ends in "/" it's considered a directory
 * *and* a regular file. Otherwise, it's just a regular file. - It is legal for
 * a key "xyz" and "xyz/" to exist at the same time. The latter is treated as a
 * directory. - If a file "a/b/c" exists but there's no "a" or "a/b/", these are
 * considered "implicit" directories. They can be listed, traversed and deleted.
 * </p>
 * <p>
 * Deviations from FileSystem provider API: - Deleting a file or directory
 * always succeeds, regardless of whether the file/directory existed before the
 * operation was issued i.e. Files.delete() and Files.deleteIfExists() are
 * equivalent.
 * </p>
 * <p>
 * Future versions of this provider might allow for a strict mode that mimics
 * the semantics of the FileSystem provider API on a best effort basis, at an
 * increased processing cost.
 * </p>
 */
public class S3FileSystemProvider extends FileSystemProvider {

    public static final String CHARSET_KEY = "s3fs_charset";
    public static final String AMAZON_S3_FACTORY_CLASS = "s3fs_amazon_s3_factory";
    public static final long PARALLEL_SIZE_THRESHOLD = 16 * 1024 * 1024l;

    private static final ConcurrentMap<String, S3FileSystem> fileSystems = new ConcurrentHashMap<>();
    private static final List<String> PROPS_TO_OVERLOAD = Arrays.asList(ACCESS_KEY, SECRET_KEY, REQUEST_METRIC_COLLECTOR_CLASS, CONNECTION_TIMEOUT, MAX_CONNECTIONS, MAX_ERROR_RETRY, PROTOCOL, PROXY_DOMAIN,
            PROXY_HOST, PROXY_PASSWORD, PROXY_PORT, PROXY_USERNAME, PROXY_WORKSTATION, SOCKET_SEND_BUFFER_SIZE_HINT, SOCKET_RECEIVE_BUFFER_SIZE_HINT, SOCKET_TIMEOUT,
            USER_AGENT, AMAZON_S3_FACTORY_CLASS, SIGNER_OVERRIDE, PATH_STYLE_ACCESS);

    private S3Utils s3Utils = new S3Utils();
    private Cache cache = new Cache();

    @Override
    public String getScheme() {
        return "s3";
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
        validateUri(uri);
        // get properties for the env or properties or system
        Properties props = getProperties(uri, env);
        validateProperties(props);
        // try to get the filesystem by the key
        String key = getFileSystemKey(uri, props);
        if (fileSystems.containsKey(key)) {
            throw new FileSystemAlreadyExistsException("File system " + uri.getScheme() + ':' + key + " already exists");
        }
        // create the filesystem with the final properties, store and return
        S3FileSystem fileSystem = createFileSystem(uri, props);
        fileSystems.put(fileSystem.getKey(), fileSystem);
        return fileSystem;
    }

    private void validateProperties(Properties props) {
        Preconditions.checkArgument(
                (props.getProperty(ACCESS_KEY) == null && props.getProperty(SECRET_KEY) == null)
                        || (props.getProperty(ACCESS_KEY) != null && props.getProperty(SECRET_KEY) != null), "%s and %s should both be provided or should both be omitted",
                ACCESS_KEY, SECRET_KEY);
    }

    private Properties getProperties(URI uri, Map<String, ?> env) {
        Properties props = loadAmazonProperties();
        addEnvProperties(props, env);
        // and access key and secret key can be override
        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            String[] keys = userInfo.split(":");
            props.setProperty(ACCESS_KEY, keys[0]);
            if (keys.length > 1) {
                props.setProperty(SECRET_KEY, keys[1]);
            }
        }
        return props;
    }

    private String getFileSystemKey(URI uri) {
        return getFileSystemKey(uri, getProperties(uri, null));
    }

    /**
     * get the file system key represented by: the access key @ endpoint.
     * Example: access-key@s3.amazonaws.com
     * If uri host is empty then s3.amazonaws.com are used as host
     *
     * @param uri   URI with the endpoint
     * @param props with the access key property
     * @return String
     */
    protected String getFileSystemKey(URI uri, Properties props) {
        // we don`t use uri.getUserInfo and uri.getHost because secret key and access key have special chars
        // and dont return the correct strings
        String uriString = uri.toString().replace("s3://", "");
        String authority = null;
        int authoritySeparator = uriString.indexOf("@");

        if (authoritySeparator > 0) {
            authority = uriString.substring(0, authoritySeparator);
        }

        if (authority != null) {
            String host = uriString.substring(uriString.indexOf("@") + 1, uriString.length());
            int lastPath = host.indexOf("/");
            if (lastPath > -1) {
                host = host.substring(0, lastPath);
            }
            if (host.length() == 0) {
                host = Constants.S3_HOSTNAME;
            }
            return authority + "@" + host;
        } else {
            String accessKey = (String) props.get(ACCESS_KEY);
            return (accessKey != null ? accessKey + "@" : "") +
                    (uri.getHost() != null ? uri.getHost() : Constants.S3_HOSTNAME);
        }
    }

    protected void validateUri(URI uri) {
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkArgument(uri.getScheme().equals(getScheme()), "uri scheme must be 's3': '%s'", uri);
    }

    protected void addEnvProperties(Properties props, Map<String, ?> env) {
        if (env == null)
            env = new HashMap<>();
        for (String key : PROPS_TO_OVERLOAD) {
            // but can be overloaded by envs vars
            overloadProperty(props, env, key);
        }

        for (String key : env.keySet()) {
            Object value = env.get(key);
            if (!PROPS_TO_OVERLOAD.contains(key)) {
                props.put(key, value);
            }
        }
    }

    /**
     * try to override the properties props with:
     * <ol>
     * <li>the map or if not setted:</li>
     * <li>the system property or if not setted:</li>
     * <li>the system vars</li>
     * </ol>
     *
     * @param props Properties to override
     * @param env   Map the first option
     * @param key   String the key
     */
    private void overloadProperty(Properties props, Map<String, ?> env, String key) {
        boolean overloaded = overloadPropertiesWithEnv(props, env, key);

        if (!overloaded) {
            overloaded = overloadPropertiesWithSystemProps(props, key);
        }

        if (!overloaded) {
            overloadPropertiesWithSystemEnv(props, key);
        }
    }

    /**
     * @return true if the key are overloaded by the map parameter
     */
    protected boolean overloadPropertiesWithEnv(Properties props, Map<String, ?> env, String key) {
        if (env.get(key) != null && env.get(key) instanceof String) {
            props.setProperty(key, (String) env.get(key));
            return true;
        }
        return false;
    }

    /**
     * @return true if the key are overloaded by a system property
     */
    public boolean overloadPropertiesWithSystemProps(Properties props, String key) {
        if (System.getProperty(key) != null) {
            props.setProperty(key, System.getProperty(key));
            return true;
        }
        return false;
    }

    /**
     * The system envs have preference over the properties files.
     * So we overload it
     * @param props Properties
     * @param key String
     * @return true if the key are overloaded by a system property
     */
    public boolean overloadPropertiesWithSystemEnv(Properties props, String key) {
        if (systemGetEnv(key) != null) {
            props.setProperty(key, systemGetEnv(key));
            return true;
        }
        return false;
    }

    /**
     * Get the system env with the key param
     * @param key String
     * @return String or null
     */
    public String systemGetEnv(String key) {
        return System.getenv(key);
    }

    /**
     * Get existing filesystem based on a combination of URI and env settings. Create new filesystem otherwise.
     *
     * @param uri URI of existing, or to be created filesystem.
     * @param env environment settings.
     * @return new or existing filesystem.
     */
    public FileSystem getFileSystem(URI uri, Map<String, ?> env) {
        validateUri(uri);
        Properties props = getProperties(uri, env);
        String key = this.getFileSystemKey(uri, props); // s3fs_access_key is part of the key here.
        if (fileSystems.containsKey(key))
            return fileSystems.get(key);
        return newFileSystem(uri, env);
    }

    @Override
    public S3FileSystem getFileSystem(URI uri) {
        validateUri(uri);
        String key = this.getFileSystemKey(uri);
        if (fileSystems.containsKey(key)) {
            return fileSystems.get(key);
        } else {
            throw new FileSystemNotFoundException("S3 filesystem not yet created. Use newFileSystem() instead");
        }
    }

    private S3Path toS3Path(Path path) {
        Preconditions.checkArgument(path instanceof S3Path, "path must be an instance of %s", S3Path.class.getName());
        return (S3Path) path;
    }

    /**
     * Deviation from spec: throws FileSystemNotFoundException if FileSystem
     * hasn't yet been initialized. Call newFileSystem() first.
     * Need credentials. Maybe set credentials after? how?
     * TODO: we can create a new one if the credentials are present by:
     * s3://access-key:secret-key@endpoint.com/
     */
    @Override
    public Path getPath(URI uri) {
        FileSystem fileSystem = getFileSystem(uri);
        /**
         * TODO: set as a list. one s3FileSystem by region
         */
        return fileSystem.getPath(uri.getPath());
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        final S3Path s3Path = toS3Path(dir);
        return new DirectoryStream<Path>() {
            @Override
            public void close() throws IOException {
                // nothing to do here
            }

            @Override
            public Iterator<Path> iterator() {
                return new S3Iterator(s3Path);
            }
        };
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        S3Path s3Path = toS3Path(path);
        String key = s3Path.getKey();

        Preconditions.checkArgument(options.length == 0, "OpenOptions not yet supported: %s", ImmutableList.copyOf(options)); // TODO
        Preconditions.checkArgument(!key.equals(""), "cannot create InputStream for root directory: %s", path);

        try {
            S3Object object = s3Path.getFileSystem().getClient().getObject(s3Path.getFileStore().name(), key);
            InputStream res = object.getObjectContent();

            if (res == null)
                throw new IOException(String.format("The specified path is a directory: %s", path));

            return res;
        } catch (AmazonS3Exception e) {
            return translateAndThrowS3Exception(e, path);
        }
    }

    private static final Set<? extends OpenOption> RANGE_SEEK_OPEN_OPTIONS = new HashSet<>(Arrays.asList(StandardOpenOption.READ, StandardOpenOption.SPARSE));

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        S3Path s3Path = toS3Path(path);
        if (RANGE_SEEK_OPEN_OPTIONS.equals(options))
        {
            return new S3RangeBasedSeekableByteChannel(s3Path);
        }
        return new S3SeekableByteChannel(s3Path, options);
    }

    @Override
    public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        S3Path s3Path = toS3Path(path);
        return new S3FileChannel(s3Path, options);
    }

    /**
     * Deviations from spec: Does not perform atomic check-and-create. Since a
     * directory is just an S3 object, all directories in the hierarchy are
     * created or it already existed.
     */
    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        S3Path s3Path = toS3Path(dir);
        Preconditions.checkArgument(attrs.length == 0, "attrs not yet supported: %s", ImmutableList.copyOf(attrs)); // TODO
        if (exists(s3Path))
            throw new FileAlreadyExistsException(format("target already exists: %s", s3Path));
        // create bucket if necesary
        Bucket bucket = s3Path.getFileStore().getBucket();
        String bucketName = s3Path.getFileStore().name();
        if (bucket == null) {
            s3Path.getFileSystem().getClient().createBucket(bucketName);
        }
        // create the object as directory
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        String directoryKey = s3Path.getKey().endsWith("/") ? s3Path.getKey() : s3Path.getKey() + "/";
        s3Path.getFileSystem().getClient().putObject(bucketName, directoryKey, new ByteArrayInputStream(new byte[0]), metadata);
    }

    @Override
    public boolean deleteIfExists(Path path) throws IOException {
        S3Path s3Path = toS3Path(path);
        deleteWithoutExistsCheck(s3Path);
        // This violates the contract for the return value. To determine if the file existed, we'd have to do a separate
        // S3 query before doing the delete. Since we don't care about whether it existed for current use-cases
        // this represents a ~50% perf win.
        return true;
    }

    @Override
    public void delete(Path path) throws IOException {
        S3Path s3Path = toS3Path(path);
        if (Files.notExists(s3Path))
            throw new NoSuchFileException("the path: " + this + " not exists");
        if (Files.isDirectory(s3Path) && Files.newDirectoryStream(s3Path).iterator().hasNext())
            throw new DirectoryNotEmptyException("the path: " + this + " is a directory and is not empty");
        deleteWithoutExistsCheck(s3Path);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        // Override to return a subclass that gives access to the S3 PUT result

        // Largely copied out of superclass
        int len = options.length;
        Set<OpenOption> opts ;
        if (len == 0) {
            opts = new HashSet<OpenOption>(Arrays.asList(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE));
        } else {
            opts = new HashSet<>();
            for (OpenOption opt: options) {
                if (opt == StandardOpenOption.READ)
                    throw new IllegalArgumentException("READ not allowed");
                opts.add(opt);
            }
            opts.add(StandardOpenOption.WRITE);
        }

        S3Path s3Path = toS3Path(path);

        // Wire up a output stream that will have access to the PUT object response from S3
        S3SeekableByteChannel wbc = new S3SeekableByteChannel(s3Path, opts);
        return new S3FSOutputStream(Channels.newOutputStream(wbc), wbc);
    }

    private void deleteWithoutExistsCheck(final S3Path s3Path) {
        final String key = s3Path.getKey();
        final String bucketName = s3Path.getFileStore().name();

        // Spin up a small thread pool to execute the two deletes in parallel
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<Object>> futures = executor.invokeAll(Arrays.asList(
                    new Callable<Object>() {
                        @Override
                        public Object call() {
                            s3Path.getFileSystem().getClient().deleteObject(bucketName, key);
                            return null;
                        }
                    },
                    new Callable<Object>() {
                        @Override
                        public Object call() {
                            // we delete the two objects (sometimes exists the key '/' and sometimes not)
                            s3Path.getFileSystem().getClient().deleteObject(bucketName, key + "/");
                            return null;
                        }
                }));
            // Thread pool will already be done will all work because invokeAll() blocks until its full list is complete
            executor.shutdownNow();
            for (Future<Object> future : futures) {
                try {
                    future.get();
                }
                catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        catch (InterruptedException e) { throw new RuntimeException(e); }
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        if (isSameFile(source, target))
            return;

        S3Path s3Source = toS3Path(source);
        S3Path s3Target = toS3Path(target);
        // TODO: implements support for copying directories

        Preconditions.checkArgument(!Files.isDirectory(source), "copying directories is not yet supported: %s", source);
        Preconditions.checkArgument(!Files.isDirectory(target), "copying directories is not yet supported: %s", target);

        ImmutableSet<CopyOption> actualOptions = ImmutableSet.copyOf(options);
        verifySupportedOptions(EnumSet.of(StandardCopyOption.REPLACE_EXISTING), actualOptions);

        if (exists(s3Target) && !actualOptions.contains(StandardCopyOption.REPLACE_EXISTING)) {
            throw new FileAlreadyExistsException(format("target already exists: %s", target));
        }

        String bucketNameOrigin = s3Source.getFileStore().name();
        String keySource = s3Source.getKey();
        String bucketNameTarget = s3Target.getFileStore().name();
        String keyTarget = s3Target.getKey();

        AmazonS3 client = s3Source.getFileSystem().getClient();
        ObjectMetadata metadata = client.getObjectMetadata(bucketNameOrigin, keySource);
        copy(client, metadata.clone(), bucketNameOrigin, keySource, bucketNameTarget, keyTarget, source);
    }

    /** Do a copy from source to target, using a TransferManager for a parallelized copy */
    public static void copy(AmazonS3 client, ObjectMetadata metadata, String sourceBucketName, String sourceKey, String targetBucketName, String targetKey, Path pathForException) throws IOException {
        try {
            final long objectSize = metadata.getContentLength();

            CopyObjectRequest request = new CopyObjectRequest(sourceBucketName, sourceKey, targetBucketName, targetKey)
                    .withNewObjectMetadata(metadata);

            if (objectSize > PARALLEL_SIZE_THRESHOLD) {
                TransferManager tm = TransferManagerBuilder.standard()
                        .withS3Client(client)
                        .withExecutorFactory(new ExecutorFactory() {
                            @Override
                            public ExecutorService newExecutor() {
                                // For files up to 1GB, use 10 parallel requests, but go to 20 for anything bigger
                                return Executors.newFixedThreadPool(objectSize > (1024L * 1024L * 1204L) ? 20 : 10);
                            }
                        })
                        .build();

                try {
                    // TransferManager processes all transfers asynchronously,
                    // so this call returns immediately.
                    Copy copy = tm.copy(request);

                    // Wait for the upload to finish before continuing.
                    try {
                        copy.waitForCompletion();
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
                finally
                {
                    tm.shutdownNow(false);
                }
            }
            else
            {
                client.copyObject(request);
            }
        }
        catch (AmazonS3Exception e) {
            translateAndThrowS3Exception(e, pathForException);
        }
    }

    public static <T> T translateAndThrowS3Exception(AmazonS3Exception e, Path path) throws IOException {
        if (e.getStatusCode() == 404) {
            NoSuchFileException notFoundException = new NoSuchFileException(e.getMessage());
            notFoundException.initCause(e);
            throw notFoundException;
        }

        // otherwise throws a generic IO exception
        throw new IOException(String.format("Problem attempting to operate on %s", path), e);
    }

    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        if (options != null && Arrays.asList(options).contains(StandardCopyOption.ATOMIC_MOVE))
            throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "Atomic not supported");
        copy(source, target, options);
        delete(source);
    }

    @Override
    public boolean isSameFile(Path path1, Path path2) throws IOException {
        return path1.isAbsolute() && path2.isAbsolute() && path1.equals(path2);
    }

    @Override
    public boolean isHidden(Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkAccess(Path path, AccessMode... modes) throws IOException {
        S3Path s3Path = toS3Path(path);
        Preconditions.checkArgument(s3Path.isAbsolute(), "path must be absolute: %s", s3Path);
        if (modes.length == 0) {
            if (exists(s3Path))
                return;
            throw new NoSuchFileException(toString());
        }

        String key = s3Utils.getS3ObjectSummary(s3Path).getKey();
        S3AccessControlList accessControlList =
                new S3AccessControlList(s3Path.getFileStore().name(), key, s3Path.getFileSystem().getClient().getObjectAcl(s3Path.getFileStore().name(), key), s3Path.getFileStore().getOwner());

        accessControlList.checkAccess(modes);
    }


    @Override
    public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type, LinkOption... options) {
        S3Path s3Path = toS3Path(path);
        if (type == BasicFileAttributeView.class) {
            return (V) new S3BasicFileAttributeView(s3Path, path);
        } else if (type == PosixFileAttributeView.class) {
            return (V) new S3PosixFileAttributeView(s3Path);
        } else if (type == null) {
            throw new NullPointerException("Type is mandatory");
        } else {
            return null;
        }
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type, LinkOption... options) throws IOException {
        S3Path s3Path = toS3Path(path);
        if (type == BasicFileAttributes.class) {
            if (cache.isInTime(s3Path.getFileSystem().getCache(), s3Path.getFileAttributes())) {
                A result = type.cast(s3Path.getFileAttributes());
                s3Path.setFileAttributes(null);
                return result;
            } else {
                S3BasicFileAttributes attrs = s3Utils.getS3FileAttributes(s3Path);
                s3Path.setFileAttributes(attrs);
                return type.cast(attrs);
            }
        } else if (type == PosixFileAttributes.class) {
            if (s3Path.getFileAttributes() instanceof PosixFileAttributes &&
                    cache.isInTime(s3Path.getFileSystem().getCache(), s3Path.getFileAttributes())) {
                A result = type.cast(s3Path.getFileAttributes());
                s3Path.setFileAttributes(null);
                return result;
            }

            S3PosixFileAttributes attrs = s3Utils.getS3PosixFileAttributes(s3Path);
            s3Path.setFileAttributes(attrs);
            return type.cast(attrs);
        }

        throw new UnsupportedOperationException(format("only %s or %s supported", BasicFileAttributes.class, PosixFileAttributes.class));
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        if (attributes == null) {
            throw new IllegalArgumentException("Attributes null");
        }

        if (attributes.contains(":") && !attributes.contains("basic:") && !attributes.contains("posix:")) {
            throw new UnsupportedOperationException(format("attributes %s are not supported, only basic / posix are supported", attributes));
        }

        if (attributes.equals("*") || attributes.equals("basic:*")) {
            BasicFileAttributes attr = readAttributes(path, BasicFileAttributes.class, options);
            return AttributesUtils.fileAttributeToMap(attr);
        } else if (attributes.equals("posix:*")) {
            PosixFileAttributes attr = readAttributes(path, PosixFileAttributes.class, options);
            return AttributesUtils.fileAttributeToMap(attr);
        } else {
            String[] filters = new String[]{attributes};
            if (attributes.contains(",")) {
                filters = attributes.split(",");
            }
            Class<? extends BasicFileAttributes> filter = BasicFileAttributes.class;
            if (attributes.startsWith("posix:")) {
                filter = PosixFileAttributes.class;
            }
            return AttributesUtils.fileAttributeToMap(readAttributes(path, filter, options), filters);
        }
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    // ~~

    /**
     * Create the fileSystem
     *
     * @param uri   URI
     * @param props Properties
     * @return S3FileSystem never null
     */
    public S3FileSystem createFileSystem(URI uri, Properties props) {
        return new S3FileSystem(this, getFileSystemKey(uri, props), getAmazonS3(uri, props), uri.getHost());
    }

    protected AmazonS3 getAmazonS3(URI uri, Properties props) {
        return getAmazonS3Factory(props).getAmazonS3(uri, props);
    }

    protected AmazonS3Factory getAmazonS3Factory(Properties props) {
        if (props.containsKey(AMAZON_S3_FACTORY_CLASS)) {
            String amazonS3FactoryClass = props.getProperty(AMAZON_S3_FACTORY_CLASS);
            try {
                return (AmazonS3Factory) Class.forName(amazonS3FactoryClass).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | ClassCastException e) {
                throw new S3FileSystemConfigurationException("Configuration problem, couldn't instantiate AmazonS3Factory (" + amazonS3FactoryClass + "): ", e);
            }
        }
        return new AmazonS3ClientFactory();
    }

    /**
     * find /amazon.properties in the classpath
     *
     * @return Properties amazon.properties
     */
    public Properties loadAmazonProperties() {
        Properties props = new Properties();
        // http://www.javaworld.com/javaworld/javaqa/2003-06/01-qa-0606-load.html
        // http://www.javaworld.com/javaqa/2003-08/01-qa-0808-property.html
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("amazon.properties")) {
            if (in != null)
                props.load(in);
        } catch (IOException e) {
            // If amazon.properties can't be loaded that's ok.
        }
        return props;
    }

    // ~~~

    private <T> void verifySupportedOptions(Set<? extends T> allowedOptions, Set<? extends T> actualOptions) {
        Sets.SetView<? extends T> unsupported = difference(actualOptions, allowedOptions);
        Preconditions.checkArgument(unsupported.isEmpty(), "the following options are not supported: %s", unsupported);
    }

    /**
     * check that the paths exists or not
     *
     * @param path S3Path
     * @return true if exists
     */
    boolean exists(S3Path path) {
        S3Path s3Path = toS3Path(path);
        try {
            s3Utils.getS3ObjectSummary(s3Path);
            return true;
        } catch (NoSuchFileException e) {
            return false;
        }
    }

    public void close(S3FileSystem fileSystem) {
        if (fileSystem.getKey() != null && fileSystems.containsKey(fileSystem.getKey()))
            fileSystems.remove(fileSystem.getKey());
    }

    public boolean isOpen(S3FileSystem s3FileSystem) {
        return fileSystems.containsKey(s3FileSystem.getKey());
    }

    /**
     * only 4 testing
     */

    protected static ConcurrentMap<String, S3FileSystem> getFilesystems() {
        return fileSystems;
    }

    public Cache getCache() {
        return cache;
    }

    public void setCache(Cache cache) {
        this.cache = cache;
    }
}