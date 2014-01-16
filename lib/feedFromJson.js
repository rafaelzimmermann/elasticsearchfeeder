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
        logger.info(type, idx, err, resp);
        future.fulfill(err, resp);
    });
    return future;
};

lineReader = flr.FileLineReader(jsonFilePath);

var readFile = function() {
    if(lineReader.hasNextLine()) {
        var line = lineReader.nextLine();
        var obj = JSON.parse(line);
        index(obj).when(function(err, data) {
            logger.info(err, data);
            readFile();
        });
    } else {
        process.exit(0);
    }
};

readFile();


