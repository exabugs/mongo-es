"use strict";

var fs = require("fs");
var async = require("async");
var request = require("request");
var mongodb = require("mongodb");
var Timestamp = mongodb.Timestamp;

var mongodb_url = "mongodb://127.0.0.1:27017/local";
var esearch_url = "http://localhost:9200";
var posfile = "./posfile";
var WAIT = 1000;

/**
 * ElasticSearch 更新
 * @param db
 * @param op
 * @param callback
 */
function update(db, op, callback) {
  var tags = op.ns.split(/^([^.]+)\./);
  var coll = db.db(tags[1]).collection(tags[2]);
  var _id = op.o._id || op.o2._id;

  async.waterfall([
    function (next) {
      if (op.op === "d") {
        next(null, _id, "DELETE", null);
      } else {
        coll.findOne({_id: _id}, function (err, obj) {
          obj && (delete obj._id);
          next(err, _id, "PUT", obj);
        });
      }
    },
    function (_id, method, obj, next) {
      var options = {
        uri: [esearch_url, op.ns, "default", _id].join("/"),
        method: method,
        json: obj
      };
      request(options, function (err) {
        next(err);
      });
    }
  ], function (err) {
    callback(err);
  });
}

/**
 * MongoDB 監視
 * @param oplog
 * @param ts
 * @param callback
 */
function loop(oplog, ts, callback) {
  var condition = {ts: {$gt: ts}};
  var option = {tailable: true};
  oplog.find(condition, option, function (err, cursor) {

    function processItem(err, op) {
      if (op) {
        // 更新処理
        update(oplog.s.db, op, function (err) {
          if (err) {
            // 更新失敗
            log(err.message);
            setTimeout(function () {
              loop(oplog, ts, callback);
            }, WAIT);
          } else {
            // 更新成功
            log("Update ElasticSearch");
            ts = op.ts;
            fs.writeFileSync(posfile, ts); // どこまで処理したか記憶する
            setImmediate(function () {
              cursor.next(processItem);
            });
          }
        });
      } else if (err && err.tailable) {
        // tailable=true なら引き続き監視可能
        log(err.message);
        setTimeout(function () {
          cursor.next(processItem);
        }, WAIT);
      } else {
        // 本当に切れた場合(MongoDB再起動等)は、findからやり直し
        log(err.message);
        setTimeout(function () {
          loop(oplog, ts, callback);
        }, WAIT);
      }
    }

    cursor.next(processItem);
  });
}

/**
 * メイン
 */
async.waterfall([
  function (next) {
    mongodb.MongoClient.connect(mongodb_url, function (err, db) {
      next(err, db);
    });
  },
  function (db, next) {
    var oplog = db.collection("oplog.rs");

    var ts;
    try {
      ts = Timestamp.fromString(fs.readFileSync(posfile, "utf-8"));
    } catch (err) {
      var now = Math.floor((new Date()).getTime() / 1000);
      ts = new Timestamp(0, now);
    }

    loop(oplog, ts, function (err) {
      next(err);
    });
  }
], function (err) {
  process.exit(err);
});

/**
 * ログ
 * @param message
 */
function log(message) {
  var now = (new Date()).toISOString();
  console.log(now + " " + message);
}
