package potternet.ConfReader;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ConfReader {
    private Map<String, Object> confMap;
    public ConfReader(String confFilePath, List<String> attrs) {
        /**
         * @param confFilePath: path to the .yaml configuration file
         * @param attrs: a attributions' list, which is used to check if .yaml configs all necessary items
         * @note: Load .yaml configuration file from specific path and analysis all parameters by the tool 'snakeyaml'
         */
        Yaml yaml = new Yaml();
        try (InputStream inputStream = Files.newInputStream(new File(confFilePath).toPath())) {
            // load attributions from .yaml
            this.confMap = yaml.load(inputStream);
            // check whether the .yaml contains all necessary attrs
            for (String tmp : attrs) {
                if (!confMap.containsKey(tmp)) {
                    System.err.printf("The .yaml file not contain the necessary attr: %s\n", tmp);
                    System.exit(2);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getAttr(String attr) {
        /**
         * @param attr: the name/key of attribution which you want to get
         * @note: Get attribution with specific name from ConfReader Object
         */
        if(!confMap.containsKey(attr)) {
            System.err.printf("Not contain the attribution named %s.\n", attr);
            System.exit(2);
        }
        return confMap.get(attr).toString();
    }
}
