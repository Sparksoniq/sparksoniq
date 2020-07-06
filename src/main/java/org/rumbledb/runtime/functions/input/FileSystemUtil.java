package org.rumbledb.runtime.functions.input;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.rumbledb.exceptions.CannotRetrieveResourceException;
import org.rumbledb.exceptions.ExceptionMetadata;
import org.rumbledb.exceptions.OurBadException;
import org.rumbledb.exceptions.RumbleException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.List;

public class FileSystemUtil {

    public static URI resolveURI(URI base, String url, ExceptionMetadata metadata) {
        if (url == null || url.isEmpty()) {
            throw new CannotRetrieveResourceException(
                    "No path provided!",
                    metadata
            );
        }
        if (!base.isAbsolute()) {
            throw new OurBadException(
                    "The base URI is not absolute!",
                    metadata
            );
        }
        try {
            return base.resolve(url);
        } catch (IllegalArgumentException e) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "Malformed URI: " + url + " Cause: " + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        }
    }

    public static URI resolveURIAgainstWorkingDirectory(String url, ExceptionMetadata metadata) {
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path workingDirectory = fileContext.getWorkingDirectory();
            URI baseUri = new URI(workingDirectory.toString() + Path.SEPARATOR + "foo");
            if (url == null || url.isEmpty()) {
                return baseUri.resolve(".");
            }
            return baseUri.resolve(url);
        } catch (UnsupportedFileSystemException e) {
            throw new CannotRetrieveResourceException(
                    "The default file system is not supported!",
                    metadata
            );
        } catch (IllegalArgumentException e) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "Malformed URI: " + url + " Cause: " + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        } catch (URISyntaxException e) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "Malformed URI: " + url + " Cause: " + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        }
    }

    public static boolean exists(URI locator, ExceptionMetadata metadata) {
        if (!locator.isAbsolute()) {
            throw new OurBadException("Unresolved uri passed to exists()");
        }
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path path = new Path(locator);
            return locator.toString().contains("*") || fileContext.util().exists(path);

        } catch (Exception e) {
            handleException(e, locator, metadata);
            return false;
        }
    }

    public static boolean delete(URI locator, ExceptionMetadata metadata) {
        checkForAbsoluteAndNoWildcards(locator, metadata);
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path path = new Path(locator);
            if (!fileContext.util().exists(path)) {
                throw new CannotRetrieveResourceException(
                        "Cannot delete file that does not exist: " + locator,
                        metadata
                );
            }
            return fileContext.delete(path, true);
        } catch (Exception e) {
            handleException(e, locator, metadata);
            return false;
        }
    }

    public static InputStream getDataInputStream(URI locator, ExceptionMetadata metadata) {
        if (locator.getScheme().equals("http") || locator.getScheme().equals("https")) {
            return getDataInputStreamHTML(locator, metadata);
        }
        checkForAbsoluteAndNoWildcards(locator, metadata);
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path path = new Path(locator);
            if (!fileContext.util().exists(path)) {
                throw new CannotRetrieveResourceException("File does not exist: " + locator, metadata);
            }
            return fileContext.open(path);
        } catch (Exception e) {
            handleException(e, locator, metadata);
            return null;
        }
    }

    public static InputStream getDataInputStreamHTML(URI locator, ExceptionMetadata metadata) {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpGet = new HttpGet(locator);
        try {
            CloseableHttpResponse response = httpclient.execute(httpGet);
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200) {
                HttpEntity entity = response.getEntity();
                return entity.getContent();
            }
            throw new CannotRetrieveResourceException(
                    "Unsuccessful status code: " + statusCode + " while requesting " + locator,
                    metadata
            );
        } catch (ClientProtocolException e) {
            handleException(e, locator, metadata);
        } catch (IOException e) {
            handleException(e, locator, metadata);
        }
        return null;
    }

    public static String readContent(URI locator, ExceptionMetadata metadata) {
        InputStream inputStream = getDataInputStream(locator, metadata);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        StringBuffer sb = new StringBuffer();
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            return sb.toString();
        } catch (Exception e) {
            handleException(e, locator, metadata);
            return null;
        }
    }

    public static void write(URI locator, List<String> content, ExceptionMetadata metadata) {
        checkForAbsoluteAndNoWildcards(locator, metadata);
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path path = new Path(locator);
            FSDataOutputStream outputStream = fileContext.create(
                path,
                EnumSet.of(CreateFlag.CREATE, CreateFlag.OVERWRITE)
            );
            for (String s : content) {
                outputStream.writeBytes(s);
                outputStream.writeBytes("\n");
            }
            outputStream.close();
        } catch (Exception e) {
            handleException(e, locator, metadata);
        }
    }

    public static void append(URI locator, List<String> content, ExceptionMetadata metadata) {
        checkForAbsoluteAndNoWildcards(locator, metadata);
        try {
            FileContext fileContext = FileContext.getFileContext();
            Path path = new Path(locator);
            FSDataOutputStream outputStream = fileContext.create(
                path,
                EnumSet.of(CreateFlag.CREATE, CreateFlag.APPEND)
            );
            for (String s : content) {
                outputStream.writeBytes(s);
                outputStream.writeBytes("\n");
            }
            outputStream.close();
        } catch (Exception e) {
            handleException(e, locator, metadata);
        }
    }

    public static void checkForAbsoluteAndNoWildcards(URI locator, ExceptionMetadata metadata) {
        if (!locator.isAbsolute()) {
            throw new OurBadException("Unresolved uri passed to append()");
        }
        if (locator.toString().contains("*")) {
            throw new CannotRetrieveResourceException(
                    "Path cannot contain *!",
                    metadata
            );
        }
    }

    private static void handleException(Throwable e, URI locator, ExceptionMetadata metadata) {
        if (e instanceof UnsupportedFileSystemException) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "No file system is configured for scheme " + locator.getScheme() + "! Cause: " + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        }
        if (e instanceof IOException) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "I/O error while accessing the "
                        + locator.getScheme()
                        + " filesystem. File: "
                        + locator
                        + " Cause: "
                        + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        }
        if (e instanceof InvocationTargetException) {
            Throwable cause = e.getCause();
            if (cause == null) {
                throw new OurBadException("Unrecognized invocation target exception: no cause.");
            }

            handleException(cause, locator, metadata);
        }
        if (e instanceof HadoopIllegalArgumentException) {
            RumbleException rumbleException = new CannotRetrieveResourceException(
                    "Illegal argument. File: " + locator + " Cause: " + e.getMessage(),
                    metadata
            );
            rumbleException.initCause(e);
            throw rumbleException;
        }
        if (e instanceof RumbleException) {
            throw (RumbleException) e;
        }
        if (e instanceof RuntimeException) {
            Throwable cause = e.getCause();
            if (cause == null) {
                throw new OurBadException("Unrecognized runtime exception: no cause. Message: " + e.getMessage());
            }
            handleException(cause, locator, metadata);
        }
        RumbleException rumbleException = new OurBadException("Unrecognized exception. Message: " + e.getMessage());
        rumbleException.initCause(e);
        throw rumbleException;
    }
}
