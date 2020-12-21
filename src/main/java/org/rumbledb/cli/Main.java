/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Authors: Stefan Irimescu, Can Berker Cikis
 *
 */
package org.rumbledb.cli;

import java.io.IOException;

import javassist.CannotCompileException;
import org.apache.spark.SparkException;
import org.rumbledb.config.RumbleRuntimeConfiguration;
import org.rumbledb.exceptions.OurBadException;
import org.rumbledb.exceptions.RumbleException;
import org.rumbledb.server.RumbleServer;
import org.rumbledb.shell.RumbleJLineShell;

public class Main {
    public static RumbleJLineShell terminal = null;

    public static void main(String[] args) throws IOException {
        RumbleRuntimeConfiguration sparksoniqConf = null;
        // Parse arguments
        try {
            sparksoniqConf = new RumbleRuntimeConfiguration(args);

            if (sparksoniqConf.isShell()) {
                launchShell(sparksoniqConf);
            } else if (sparksoniqConf.isServer()) {
                launchServer(sparksoniqConf);
            } else if (sparksoniqConf.getQueryPath() != null) {
                runQueryExecutor(sparksoniqConf);
            } else {
                System.out.println("    ____                  __    __   ");
                System.out.println("   / __ \\__  ______ ___  / /_  / /__ ");
                System.out.println("  / /_/ / / / / __ `__ \\/ __ \\/ / _ \\");
                System.out.println(" / _, _/ /_/ / / / / / / /_/ / /  __/");
                System.out.println("/_/ |_|\\__,_/_/ /_/ /_/_.___/_/\\___/ ");
                System.out.println("Usage:");
                System.out.println("spark-submit <Spark arguments> <path to rumble jar> <Rumble arguments>");
                System.out.println("");
                System.out.println("Example usage:");
                System.out.println("spark-submit spark-rumble-1.0.jar --shell yes");
                System.out.println("spark-submit --master local[*] spark-rumble-1.7.0.jar --shell yes");
                System.out.println("spark-submit --master local[2] spark-rumble-1.7.0.jar --shell yes");
                System.out.println(
                    "spark-submit --master local[*] --driver-memory 10G spark-rumble-1.0.jar --shell yes"
                );
                System.out.println("");
                System.out.println("spark-submit --master yarn spark-rumble-1.7.0.jar --shell yes");
                System.out.println(
                    "spark-submit --master yarn --executor-cores 3 --executor-memory 5G spark-rumble-1.7.0.jar --shell yes"
                );
                System.out.println("spark-submit --master local[*] spark-rumble-1.7.0.jar --query-path my-query.jq");
                System.out.println("spark-submit --master local[*] spark-rumble-1.7.0.jar --query-path my-query.jq");
                System.out.println(
                    "spark-submit --master yarn --executor-cores 3 --executor-memory 5G spark-rumble-1.7.0.jar --query-path hdfs://server:port/my-query.jq --output-path hdfs://server:port/my-output.json"
                );
                System.out.println(
                    "spark-submit --master local[*] spark-rumble-1.0.jar --query-path my-query.jq --output-path my-output.json --log-path my-log.txt"
                );
            }
            System.exit(0);
        } catch (Exception ex) {
            boolean showErrorInfo = false;
            if (sparksoniqConf != null) {
                showErrorInfo = sparksoniqConf.getShowErrorInfo();
            }
            handleException(ex, showErrorInfo);
        }
    }

    private static void handleException(Throwable ex, boolean showErrorInfo) {
        if (ex != null) {
            if (ex instanceof SparkException) {
                Throwable sparkExceptionCause = ex.getCause();
                if (sparkExceptionCause != null) {
                    handleException(sparkExceptionCause, showErrorInfo);
                } else {
                    handleException(new RumbleException(ex.getMessage()), showErrorInfo);
                }
            } else if (ex instanceof RumbleException && !(ex instanceof OurBadException)) {
                System.err.println("⚠️  ️" + ex.getMessage());
                if (showErrorInfo) {
                    ex.printStackTrace();
                }
                System.exit(42);
            } else if (ex instanceof IllegalArgumentException) {
                System.err.println(
                    "⚠️  There was an IllegalArgumentException. Most of the time, this happens because you are not using Java 8. Spark only works with Java 8."
                );
                System.err.println(
                    "If you have several versions of java installed, you need to set your JAVA_HOME accordingly."
                );
                System.err.println("If you do not have Java 8 installed, we recommend installing AdoptOpenJDK 1.8.");
                System.err.println(
                    "For more debug info, please try again using --show-error-info yes in your command line."
                );
                if (showErrorInfo) {
                    ex.printStackTrace();
                }
                System.exit(43);
            } else if (ex instanceof CannotCompileException) {
                System.err.println("⚠️  There was a CannotCompileException.");
                System.err.println(
                    "There is a known issue with this on Docker and on certain versions of OpenJDK due to the JSONiter library."
                );
                System.err.println(
                    "We have a workaround: please try again using --deactivate-jsoniter-streaming yes on your command line. json-doc() will, however, not be available."
                );
                System.err.println(
                    "For more debug info, please try again using --show-error-info yes in your command line."
                );
                if (showErrorInfo) {
                    ex.printStackTrace();
                }
                System.exit(43);
            } else {
                System.err.println("An error has occured: " + ex.getMessage());
                System.err.println(
                    "We should investigate this 🙈. Please contact us or file an issue on GitHub with your query."
                );
                System.err.println("Link: https://github.com/RumbleDB/rumble/issues");
                System.err.println(
                    "For more debug info (e.g., so you can communicate it to us), please try again using --show-error-info yes in your command line."
                );
                if (showErrorInfo) {
                    ex.printStackTrace();
                }
                System.exit(-42);
            }
        }
    }

    private static void runQueryExecutor(RumbleRuntimeConfiguration sparksoniqConf) throws IOException {
        JsoniqQueryExecutor translator = new JsoniqQueryExecutor(sparksoniqConf);
        translator.runQuery();
    }

    private static void launchShell(RumbleRuntimeConfiguration sparksoniqConf) throws IOException {
        terminal = new RumbleJLineShell(sparksoniqConf);
        terminal.launch();
    }

    private static void launchServer(RumbleRuntimeConfiguration sparksoniqConf) throws IOException {
        RumbleServer server = new RumbleServer(sparksoniqConf);
        server.start();
    }

    public static void printMessageToLog(String message) {
        if (Main.terminal == null) {
            System.out.println(message);
        } else {
            Main.terminal.output(message);
        }
    }

}
