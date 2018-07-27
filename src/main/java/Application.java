import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class Application {

    public static final String PROPERTIES = "application.properties";
    public static final SimpleDateFormat DATE_PROP_FORMAT = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
    public static final SimpleDateFormat DATE_NAME_FORMAT = new SimpleDateFormat("ddMMyyyyHHmmss");
    public static final Pattern LOG_DELIMITER = Pattern.compile("\\*+");
    public static final Pattern UTTRNO = Pattern.compile("\\s([0-9]{12,13})\\s+");
    public static final String DATA_SEPARATOR = "--------------------------------------------- DATASEPARATOR -----------------------------------------------------------\n";

    private final String utrnno;
    private final Date dateStart;
    private final Date dateEnd;
    private final String logPath;
    private final String outPath;
    private final Boolean zip;

    public final Object folderWriteMonitor = new Object();

    private final Map<String, Runnable> runnables = new HashMap<String, Runnable>() {{
        put("findByUTRNNOAndExtract", () -> findByUTRNNOAndExtract());
        put("findAllAndExtract", () -> findAllAndExtract());
        put("findAllInInterval", () -> findAllInInterval());
        put("zip", () -> zip());
    }};

    public Application(String propertiesPath) throws IOException, ParseException {
        Properties props = new Properties();
        props.load(new FileInputStream(new File(propertiesPath)));

        logPath = props.getProperty("log.path");
        outPath = props.getProperty("out.path");
        utrnno = props.getProperty("utrnno");
        zip = Boolean.valueOf(props.getProperty("zip", "false"));
        dateStart = DATE_PROP_FORMAT.parse(props.getProperty("date.start"));
        dateEnd = DATE_PROP_FORMAT.parse(props.getProperty("date.end"));

        System.out.println("**************** PROPERTIES ****************");
        System.out.println("log.path: " + logPath);
        System.out.println("out.path: " + outPath);
        System.out.println("utrnno: " + utrnno);
        System.out.println("date.start: " + dateStart);
        System.out.println("date.end: " + dateEnd);
        System.out.println("zip: " + zip);
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("******************* HINT *******************");
            System.out.println("You may use that:> java -jar ipc-logparser.jar %PATH_TO_PROPERTIES%");
        }

        try {
            String properties = args.length > 0 && args[0] != null ? args[0] : PROPERTIES;
            new Application(properties).run();
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            ex.printStackTrace();
        }
    }

    public void run() {
        System.out.println("******************* RUN ********************");
        if (utrnno != null && !utrnno.trim().isEmpty()) {
            runnables.get("findByUTRNNOAndExtract").run();
        } else {
            runnables.get("findAllInInterval").run();
        }

        if (zip) {
            runnables.get("zip").run();
        }
        System.out.println("******************* DONE *******************");
    }

    /**
     * find text blocks in the interval
     */
    private void findAllInInterval() {
        List<File> logFiles = extractFilesFromFolder(logPath);
        System.out.println("Found " + logFiles.size() + " files for search");
        AtomicInteger resultCounter = new AtomicInteger(0);
        logFiles.parallelStream().forEach(file -> {
            try {
                Path pathFile = Paths.get(outPath + File.separator + file.getPath());
                // folder for storage
                File resultFolder = getResultFolder(pathFile.toString()
                        .replaceFirst(outPath, "")
                        .replaceFirst(file.getName(), ""));
                // get real file path
                pathFile = Paths.get(resultFolder.getPath() + File.separator + file.getName());
                // delete old data
                if (Files.exists(pathFile)) {
                    Files.delete(pathFile);
                }

                try (FileWriter fileWriter = new FileWriter(pathFile.toFile())) {
                    for (String row : new String(Files.readAllBytes(file.toPath())).split("\n")) {
                        // check date
                        Date rowDate = getRowDate(row);
                        if (rowDate.after(dateStart) && rowDate.before(dateEnd)) {
                            // write to file
                            fileWriter.write(row);
                            fileWriter.write("\n");
                        }
                    }
                }

                // count file that we wrote
                if (pathFile.toFile().exists() && pathFile.toFile().isFile()) {
                    resultCounter.incrementAndGet();
                }
            } catch (IOException ex) {
                System.err.println("IOException: " + ex.getMessage());
            }
        });

        System.out.println("Found " + resultCounter.get() + " files in interval [" + dateStart + " - " + dateEnd + "]");
    }

    /**
     * find transactions and save results
     */
    private void findAllAndExtract() {
        List<File> logFiles = extractFilesFromFolder(logPath);
        System.out.println("Found " + logFiles.size() + " files for search");
        Map<String, AtomicInteger> resultCounter = new ConcurrentHashMap<>();
        logFiles.parallelStream().forEach(file -> {
            try {
                findData(new String(Files.readAllBytes(file.toPath()))).forEach((utrnno, dataBlock) -> {
                    if (dataBlock != null && !dataBlock.isEmpty()) {
                        saveDataBlock(file.getName(), utrnno, dataBlock);
                        resultCounter.put(utrnno, new AtomicInteger(
                                resultCounter
                                        .getOrDefault(utrnno, new AtomicInteger(0))
                                        .incrementAndGet()
                        ));
                    }
                });
            } catch (IOException ex) {
                System.err.println("Failed read file: " + ex.getMessage());
            }
        });

        resultCounter.forEach((utrnno, count) -> System.out.println("Found " + count + " files with [UTRNNO:" + utrnno + "]"));
    }

    /**
     * find specific UTTRNO and save results
     */
    private void findByUTRNNOAndExtract() {
        List<File> logFiles = extractFilesFromFolder(logPath);
        System.out.println("Found " + logFiles.size() + " files for search");

        AtomicInteger resultCounter = new AtomicInteger(0);
        logFiles.parallelStream().forEach(file -> {
            try {
                String dataBlock = findData(new String(Files.readAllBytes(file.toPath())), utrnno).get(utrnno);
                if (dataBlock != null && !dataBlock.isEmpty()) {
                    saveDataBlock(file.getName(), utrnno, dataBlock);
                    resultCounter.incrementAndGet();
                }
            } catch (IOException ex) {
                System.err.println("Failed read file: " + ex.getMessage());
            }
        });

        if (resultCounter.get() > 0) {
            System.out.println("Found " + resultCounter.get() + " files with [UTRNNO:" + utrnno + "]");
        }
    }

    /**
     * archive results
     */
    private void zip() {
        System.out.println("******************* ZIP ********************");
        try {
            saveFileToZIPArchive(outPath);
        } catch (IOException ex) {
            System.err.println("ZIP fail: " + ex.getMessage());
        }
    }

    /**
     * get date for the current raw
     *
     * @param row text raw
     * @return {@link Date}
     */
    private Date getRowDate(String row) {
        Date currentDate = new Date(0);
        if ((" " + row).indexOf(">>") > 0 && row.indexOf(">>") < 4 && row.indexOf(":") > 0 && row.indexOf(":") < 20) {
            try {
                String time = row.substring(row.indexOf(">>") + 2, row.indexOf(":"));
                long timestamp = (long) (Double.parseDouble(time) * 1000);
                currentDate = new Date(timestamp);
            } catch (NumberFormatException ex) {
                System.err.println(ex.getMessage() + " - failed parse time");
            }
        }

        return currentDate;
    }

    /**
     * recursively extract all files from the path
     *
     * @param path
     * @return лист {@link List} файлов
     */
    private List<File> extractFilesFromFolder(String path) {
        File folder = new File(path);
        if (!folder.exists() || !folder.isDirectory()) {
            throw new RuntimeException("[" + path + "] - isn't folder");
        }

        if (folder.listFiles() == null) {
            throw new RuntimeException("[" + path + "] - is empty");
        }

        List<File> files = new ArrayList<>();
        Arrays.asList(folder.listFiles()).forEach(file -> {
            if (!file.isDirectory()) {
                files.add(file);
            } else {
                try {
                    files.addAll(extractFilesFromFolder(file.getPath()));
                } catch (Exception ex) {
                    System.err.println(ex.getMessage());
                }
            }
        });

        return files;
    }

    /**
     * save info about transactions that was found
     *
     * @param source    source of info
     * @param utrnno    number of transaction
     * @param dataBlock data
     */
    private void saveDataBlock(String source, String utrnno, String dataBlock) {
        // get folder for storage
        File resultFolder = getResultFolder(utrnno);
        // write results
        try {
            Path pathFile = Paths.get(resultFolder.getPath() + File.separator
                    + source + "_" + DATE_NAME_FORMAT.format(dateStart) + "_" + DATE_NAME_FORMAT.format(dateEnd));
            if (Files.exists(pathFile)) {
                Files.delete(pathFile);
            }
            Files.write(pathFile, dataBlock.getBytes(), StandardOpenOption.CREATE);
        } catch (IOException ex) {
            System.err.println("Failed write file: " + ex.getMessage());
        }
    }

    /**
     * create folder to store results
     *
     * @param path number of trans or just path to identify folder
     * @return {@link} folder to store results
     */
    private File getResultFolder(String path) {
        File resultFolder = new File(outPath + File.separator + path);
        synchronized (folderWriteMonitor) {
            if (!resultFolder.exists() || !resultFolder.isDirectory()) {
                if (!resultFolder.mkdir() && !resultFolder.mkdirs()) {
                    System.err.println("Failed mkdir: " + resultFolder.getPath());
                }
            }
        }

        return resultFolder;
    }

    /**
     * search blocks of data with trans
     *
     * @param content content for search
     * @return {@link Map} (utrrnno, block)
     */
    private Map<String, String> findData(String content) {
        return findData(content, null);
    }

    /**
     * search data blocks with utrnno
     *
     * @param content content for search
     * @return {@link Map} (utrrnno, block)
     */
    private Map<String, String> findData(String content, String utrnno) {
        String[] rows = content.split("\n");
        Map<String, String> result = new HashMap<>();

        int lastFindPos = 0;
        for (int currPos = 0; currPos < rows.length; currPos++) {
            String row = rows[currPos];

            // check date
            Date rowDate = getRowDate(row);
            if (!rowDate.after(dateStart) && !rowDate.before(dateEnd)) continue;

            // determine a trans that we want to find
            String searchUtrnno = null;
            if (utrnno == null || utrnno.isEmpty()) {
                Matcher matcher = UTTRNO.matcher(row);
                if (matcher.find()) {
                    searchUtrnno = matcher.group();
                }
                if (searchUtrnno == null || searchUtrnno.isEmpty()) continue;
            } else {
                searchUtrnno = utrnno;
            }
            searchUtrnno = searchUtrnno.trim();

            if (!row.contains(" " + searchUtrnno + " ")) continue;

            int startBlock = currPos;
            startBlock = startBlock > 0 ? startBlock : 0;
            startBlock = startBlock > lastFindPos ? startBlock : lastFindPos + 1;

            int endBlock = currPos + 20;
            endBlock = endBlock <= rows.length ? endBlock : rows.length;

            StringBuilder search = new StringBuilder();
            search.append(DATA_SEPARATOR);
            for (int i = startBlock; i < endBlock; i++) {
                if ((rows[i].contains(searchUtrnno) && i != currPos && currPos > startBlock + 10)
                        || LOG_DELIMITER.matcher(rows[i]).find()) break;
                search.append(rows[i]);
                search.append("\n");
            }

            if (!search.toString().isEmpty()) {
                lastFindPos = currPos;
                search.append(DATA_SEPARATOR);
                result.put(searchUtrnno, result.getOrDefault(searchUtrnno, "") + search.toString());
            }
        }

        return result;
    }

    /**
     * zip package
     *
     * @param pathDestination File
     * @throws IOException disk error
     */
    private void saveFileToZIPArchive(String pathDestination) throws IOException {
        System.out.println("Start create ZIP-archive");
        File file = new File(pathDestination);
        if (!file.exists()) {
            System.err.println(file.getPath() + " is missing");
            return;
        }

        String zipName = String.format("%s.%s", file.getPath(), "zip");
        Path path = Paths.get(zipName);
        if (Files.exists(path)) {
            Files.delete(path);
        }
        System.out.println("Try create ZIP: " + zipName);
        try (
                FileInputStream fileInputStream = new FileInputStream((Files.createFile(path)).toFile());
                ZipOutputStream zipOutputStream = new ZipOutputStream(new FileOutputStream(path.toFile()))
        ) {
            int count;
            byte[] buffer = new byte[1024];
            if (file.isDirectory()) {
                for (File item : extractFilesFromFolder(file.getPath())) {
                    zipOutputStream.putNextEntry(
                            new ZipEntry(item.getPath().replace(file.getPath() + File.separator, ""))
                    );
                    try (FileInputStream in = new FileInputStream(item.getPath())) {
                        while ((count = in.read(buffer)) > 0) {
                            zipOutputStream.write(buffer, 0, count);
                        }
                    }
                }
            } else {
                zipOutputStream.putNextEntry(new ZipEntry(zipName));
                while ((count = fileInputStream.read(buffer)) > 0) {
                    zipOutputStream.write(buffer, 0, count);
                }
            }
        } finally {
            System.out.println(path + " created");
        }

        // clear folder with data after package
        if (file.isDirectory()) {
            extractFilesFromFolder(file.getPath()).parallelStream().forEach(File::delete);
        } else {
            file.delete();
        }
        System.out.println("Clear result files");
    }
}