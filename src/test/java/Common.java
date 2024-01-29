import org.apache.commons.io.IOUtils;

import java.io.IOException;

public class Common {
    public static String getStmtFromFile(String filePath) throws IOException {
        return IOUtils.toString(
                Common.class.getResourceAsStream(filePath),
                "UTF-8"
        );
    }
}
