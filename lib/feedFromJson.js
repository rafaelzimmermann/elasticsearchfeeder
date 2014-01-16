var lib = process.cwd() + "/lib/",
    
    flr = require(lib + "helper/filelinereader")
    
    futures = require("futures"),
    _ = require("underscore"),


    env = require(lib + "helper/environment"),
    logger = require(lib + "helper/logger"),

    elasticsearch = require("elasticsearch");

var jsonFilePath = process.argv[2],
    type = process.argv[3],
    idx = process.argv[4];

logger.info(jsonFilePath, type, idx);

var esClient = elasticsearch.Client({
  hosts: env.elasticsearch.hosts
});

var index = function(body) {
    var future = futures.future.create();
    
    if (_.isEmpty(body)) {
        logger.error("Body was empty");
        future.fulfill(true, null);
    }

    var obj = {}
    
    obj["index"] = idx;
    obj["type"] = type;
    obj["body"] = body;
    logger.info(obj);

    esClient.index(obj, function(err, resp) {
        future.fulfill(err, resp);
    });
    return future;
};

lineReader = flr.FileLineReader(jsonFilePath);

var readFile = function() {
    if(lineReader.hasNextLine()) {
        var line = lineReader.nextLine();
        try {
            var obj = JSON.parse(line);
            if (!_.isEmpty(obj)) {
                index(obj).when(function(err, data) {
                    logger.info(err, data);
                    readFile();
                });
            }
        } catch(ex) {
            logger.error("Failed parsing line");
            readFile();
        }
    } else {
        process.exit(0);
    }
};

readFile();


