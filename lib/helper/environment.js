var nconf = require("nconf");

nconf.argv().env().file({file: "config.json"}).file("package", "package.json");

var onsiteVersion = nconf.get("version");

var nodeEnv = "development";
var Environment = nconf.get(nodeEnv);


if (Environment) {
    console.info("Using NODE_ENV:", nodeEnv, JSON.stringify(Environment));
    Environment["version"] = onsiteVersion;

    module.exports = Environment;
} else {
    console.error("Could not load:", nodeEnv);
    console.error("Exiting...");

    process.exit(1);
}
