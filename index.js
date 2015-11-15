"use strict";

var fs = require("fs");
var async = require("async");
var request = require("request");
var mongodb = require("mongodb");
var Timestamp = mongodb.Timestamp;

var mongodb_url = "mongodb://127.0.0.1:27017/local";
var esearch_url = "http://localhost:9200";
var posfile = "./logs/pos";

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

function loop(oplog, ts, callback) {
  var condition = {ts: {$gt: ts}};
  var option = {tailable: true};
  oplog.find(condition, option, function (err, cursor) {

    function processItem(err, op) {
      if (op) {
        ts = op.ts;
        fs.writeFileSync(posfile, ts);
        update(oplog.s.db, op, function () {
          cursor.next(processItem);
        });
      } else if (err && !err.tailable) {
        console.log(err.message);
        setTimeout(function () {
          loop(oplog, ts, callback);
        }, 1000);
      } else {
        console.log((new Date()).toISOString() + " " + err.message);
        setTimeout(function () {
          cursor.next(processItem);
        }, op ? 0 : 1000);
      }
    }

    cursor.next(processItem);
  });
}

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
      ts = Timestamp.fromString(fs.readFileSync(posfile).toString());
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
