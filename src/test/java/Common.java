import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;

public class Common {
    public static String getStmtFromFile(String filePath) throws IOException {
        return IOUtils.toString(
                Common.class.getResourceAsStream(filePath),
                "UTF-8"
        );
    }

    public static String getAbsolutePathFromName(String fileName) {
        ClassLoader classLoader = Common.class.getClassLoader();
        File file = new File(classLoader.getResource(fileName).getFile());
        return file.getAbsolutePath();
    }
}
