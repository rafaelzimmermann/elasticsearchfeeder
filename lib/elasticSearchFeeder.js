var lib = process.cwd() + "/lib/",
    futures = require("futures"),
    _ = require("underscore"),

    env = require(lib + "helper/environment"),
    logger = require(lib + "helper/logger"),

    elasticsearch = require("elasticsearch"),
    cassandra = require("cassandra-client");

var cassandraEnv = env.cassandra;

var esClient = elasticsearch.Client({
  hosts: env.elasticsearch.hosts
});

var limit = 1;


var getColumnFamilies = function() {
    var future = futures.future.create();
    var sys = new cassandra.System(cassandraEnv.host + ":" + cassandraEnv.port);
    var cfs = [];
    sys.describeKeyspace(cassandraEnv.keyspace, function(err, ksDef) {
        if (err) {
            logger.error("Error while fetching keyspace definition: " + err);
        } else {
            if (ksDef.cf_defs.length) {
                logger.info(ksDef);
                ksDef.cf_defs.forEach(function(def) {
                    cfs.push(def.name);
                });
            }
        }
        future.fulfill(err, cfs);
    });
    return future;
};

var indexRow = function(row, type, index) {
    var body = {};
    var ojb = {}
    var future = futures.future.create();
    _.each(row.cols, function(c) {
        body[c.name] = c.value;
    });
    ojb["index"] = index;
    ojb["type"] = type;
    ojb["body"] = body;
    logger.info(row.key, _.keys(ojb));
    esClient.index(ojb, function(err, resp) {
        logger.info(type, index, err, resp);
        future.fulfill(err, resp);
    });
    return future;
}

var indexColumnFamily = function(con, cf, index) {
    logger.info(cf, index);
    var future = futures.future.create();
    var join;
    console.log(index, limit, index + limit);
    con.execute('SELECT * FROM ? WHERE key = ?', [cf, index], function (err, rows) {

        if (err || rows.length <= 0) {
          logger.error("Failed during query.",err);
          process.exit(0);
        } else {
            rows = rows.slice(0, limit);
            join = futures.join.create();
            rows.forEach(function(r) {
                join.add(indexRow(r, cf, r.key));
            });
            join.when(function(err, resp) {
                future.fulfill(err, resp);
            });

            // indexRow(rows[0], cf, index).when(function() {
            //     if (rows.rowCount() === limit) {
            //         indexColumnFamily(con, cf, index+limit);
            //     } else {
            //         future.fulfill();
            //     }
            // });
        }
    });
    return future;
}

indexColumnFamilies = function(con, cfs) {
    indexColumnFamily(con, cfs[0], 0).when(function() {
        if(cfs.length > 1) {
            indexColumnFamilies(con, cfs.slice(1));
        }
    })
}

futures.sequence.create().
    then(function(next) {
        getColumnFamilies().when(next);
    }).
    then(function(next, err, columnFamilies) {

        if (err || !columnFamilies) {
            logger.error("Sothing smells.");
            process.exit(0);
        } else {
            logger.info("columnFamilies: "+columnFamilies.join(", "));
            var Connection = cassandra.Connection;
            var con = new Connection(cassandraEnv);
            con.connect(function(err) {
                indexColumnFamilies(con, columnFamilies.slice(2));
            });
        }

    });


