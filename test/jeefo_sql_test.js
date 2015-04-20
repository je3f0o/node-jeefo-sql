"use strict";

var async   = require("async"),
	mysql   = require("mysql"), jeefoDB = require("../lib/jeefo_sql"),
	moment  = require("moment"),
	Chance  = require("chance"),

	c = new Chance(),
	test_db_config = {
		adapter     : "mysql",
		host        : "127.0.0.1",
		user        : "root",
		password    : "",
		db_name     : "testee",
		dataStrings : true
	},

	date_format = "YYYY-MM-DD",

	testRecord1 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) },
	testRecord2 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) },
	testRecord3 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) };

function operator_test(record, query, callback) {
	var db = jeefoDB(test_db_config, "users");
	db.insert(record, function () {
		db.find(query, callback);
	});
}

exports["jeefo-db"] = {
	setUp: function (done) {
		var mysql_conn;

		async.waterfall(
			[
				function (cb) {
					var mysql_config = {
						host        : test_db_config.host,
						user        : test_db_config.user,
						password    : test_db_config.password,
						database    : test_db_config.db_name
					};
					mysql_conn = mysql.createConnection(mysql_config);
					cb();
				},
				function (cb) {
					mysql_conn.query("DELETE FROM `users`", [], function () {
						mysql_conn.query("ALTER TABLE `users` AUTO_INCREMENT = 1", [], cb);
					});
				},
				function (result, fields, cb) {
					mysql_conn.query("DELETE FROM `customers`", [], function () {
						mysql_conn.query("ALTER TABLE `customers` AUTO_INCREMENT = 1", [], cb);
					});
				},
				function (result, fields, cb) {
					mysql_conn.query(
						"INSERT INTO `users`(`firstname`, `lastname`, `age`, `birthday`) VALUES(?, ?, ?, ?);",
						[ testRecord1.firstname, testRecord1.lastname, testRecord1.age, testRecord1.birthday ],
						cb
					);
				},
				function (result, fields, cb) {
					mysql_conn.query(
						"INSERT INTO `users`(`firstname`, `lastname`, `age`, `birthday`) VALUES(?, ?, ?, ?);",
						[ testRecord2.firstname, testRecord2.lastname, testRecord2.age, testRecord2.birthday ],
						cb
					);
				},
				function (result, fields, cb) {
					mysql_conn.query(
						"INSERT INTO `users`(`firstname`, `lastname`, `age`, `birthday`) VALUES(?, ?, ?, ?);",
						[ testRecord3.firstname, testRecord3.lastname, testRecord3.age, testRecord3.birthday ],
						cb
					);
				}
			],
			function () {
				this.db = jeefoDB(test_db_config, "users");
				mysql_conn.end(done);
			}.bind(this)
		);
	},
	tearDown: function (done) {
		var mysql_conn;

		async.waterfall(
			[
				function (cb) {
					mysql_conn = mysql.createConnection(test_db_config);
					cb();
				},
				function (cb) {
					mysql_conn.query("DELETE FROM `users`;", [], cb);
				},
				function (result, fields, cb) {
					mysql_conn.query("DELETE FROM `customers`;", [], cb);
				}
			],
			function () {
				mysql_conn.end(done);
			}
		);
	},
	"Can create jeefo-db instance": function (test) {
		var db;
		test.doesNotThrow(function () {
			db = jeefoDB(test_db_config, "users");
		});
		test.notEqual(db, void 0, "should be instance of JeefoDB");
		test.done();
	},
	"Can insert record": function (test) {
		this.db.insert(testRecord1, function (err, record) {
			test.equal(record.id, 4, "id should be 4");
			test.equal(testRecord1.firstname, record.firstname, "names should be equal");
			test.done();
		});
	},
	"Can find records by one key": function (test) {
		this.db.find({ firstname: testRecord1.firstname }, function (err, result) {
			test.equal(result.records.length, 1, "should find 1 record");
			test.equal(testRecord1.firstname, result.records[0].firstname, "names should be equal");
			test.done();
		});
	},
	"Can find records by multiple keys": function (test) {
		this.db.find({ firstname: testRecord1.firstname, age: testRecord1.age }, function (err, result) {
			test.equal(result.records.length, 1, "should find 1 record");
			test.equal(testRecord1.firstname, result.records[0].firstname, "names should be equal");
			test.equal(testRecord1.age, result.records[0].age, "ages should be equal");
			test.done();
		});
	},
	"Can find multiple records": function (test) {
		var db = this.db,
			r = { firstname: testRecord1.firstname, age: c.age() };

		db.insert(r, function () {
			db.find({ firstname: testRecord1.firstname }, function (err, result) {
				test.equal(result.records.length, 2, "2 records should be found");
				test.done();
			});
		});
	},
	"Query operators": {
		"$lt": function (test) {
			operator_test({ age: 10 }, { age: { $lt: 11 } }, function (err, result) {
				test.notEqual(result.records.length, 0, "should have at least 1 record");
				test.done();
			});
		},
		"$gt": function (test) {
			operator_test({ age: 10 }, { age: { $gt: 9 } }, function (err, result) {
				test.notEqual(result.records.length, 0, "should have at least 1 record");
				test.done();
			});
		},
		"$lte": function (test) {
			operator_test({ age: 10 }, { age: { $lte: 10 } }, function (err, result) {
				test.notEqual(result.records.length, 0, "should have at least 1 record");
				test.done();
			});
		},
		"multiple operators": function (test) {
			operator_test({ age: 10 }, { age: { $lt: 11, $gt: 9, $gte: 10 } }, function (err, result) {
				test.notEqual(result.records.length, 0, "should have at least 1 record");
				test.done();
			});
		},
		"$limit object" : function (test) {
			var db = this.db;

			db.insert(testRecord1, function () {
				db.find({
					$limit : {
						offset : 1,
						max : 2
					}
				}, function (err, result) {
					test.equal(result.records[0].id, 2, "first record.id should be 2");
					test.equal(result.records.length, 2, "records.length should be 2");
					test.done();
				});
			});
		},
		"$limit max number" : function (test) {
			var db = this.db;

			db.insert(testRecord1, function () {
				db.find({ $limit : 2 }, function (err, result) {
					test.equal(result.records[0].id, 1, "first record.id should be 1");
					test.equal(result.records.length, 2, "records.length should be 2");
					test.done();
				});
			});
		},
		"$order_by" : function (test) {
			var db = this.db;

			db.first({
				$order_by : {
					id : "desc"
				}
			}, function (err, result) {
				test.equal(result.record.id, 3, "id should be 3");
				test.done();
			});
		},
		"$like" : function (test) {
			var db = this.db,
				record = {
					firstname : "Washington"
				}

			db.insert(record, function () {
				db.first({
					firstname : {
						$like : "%ashingto%"
					}
				}, function (err, data) {
					test.equal(data.record.firstname, "Washington", "firstname should be Washington");
					test.done();
				});
			});
		}
	},
	"Can get a first record": function (test) {
		this.db.first(function (err, result) {
			test.equal(result.record.id, 1, "record.id should be 1");
			test.done();
		});
	},
	"Can get a last record": function (test) {
		this.db.last(function (err, result) {
			test.equal(result.record.id, 3, "record.id should be 3");
			test.done();
		});
	},
	"Can find a single record": function (test) {
		this.db.first({ firstname: testRecord1.firstname }, function (err, result) {
			test.equal(result.record.firstname, testRecord1.firstname, "names should be equal");
			test.done();
		});
	},
	"Can find all records": function (test) {
		this.db.find(function (err, result) {
			test.equal(result.records.length, 3, "should find 3 records");
			test.done();
		});
	},
	"Can find IN method records": function (test) {
		this.db.find({ id : [1, 2] }, function (err, result) {
			test.equal(result.records.length, 2, "should find 2 records");
			test.done();
		});
	},
	"Can find IN method empty array": function (test) {
		this.db.find({ id : [] }, function (err, result) {
			test.equal(result.records.length, 0, "should find 0 records");
			test.equal(result.total, 0, "should find 0 records");
			test.done();
		});
	},
	"Can get total results number": function (test) {
		this.db.total({ firstname : testRecord1.firstname }, function (err, total) {
			test.equal(total, 1, "should at least 1 records");
			test.done();
		});
	},
	"Can update records": function (test) {
		this.db.update({ firstname: testRecord1.firstname }, { age: 200 }, function (err, records) {
			test.notEqual(records.length, 0, "should have at least 1 record updated");
			records.forEach(function (record) {
				test.equal(record.age, 200, "age should be updated");
			});
			test.done();
		});
	},
	"Can delete records": function (test) {
		var db = this.db;
		
		db.delete({ firstname: testRecord1.firstname }, function () {
			db.find({ firstname: testRecord1.firstname }, function (err, result) {
				test.equal(result.records.length, 0, "no records should be found");
				test.done();
			});
		});
	},
	"Can change table name": function (test) {
		var db = this.db,
			r = { name: c.first() };

		db.set_table("customers").insert(r, function () {
			db.first({ name : r.name }, function (err, result) {
				test.equal(result.record.name, r.name, "names should be equal");
				test.done();
			});
		});
	},
	"Can Database open close test": function (test) {
		var db = this.db, results = {};

		test.expect(9);

		async.waterfall(
			[
				function (cb) {
					db.open(false, cb);
				},
				function (cb) {
					db.insert(testRecord1, function (err, record) {
						results.insert = record;
						cb();
					});
				},
				function (cb) {
					db.find(testRecord1, function (err, result) {
						results.find = result.records;
						cb();
					});
				},
				function (cb) {
					db.first(testRecord1, function (err, result) {
						results.first = result.record;
						cb();
					});
				},
				function (cb) {
					test.equal(results.insert.firstname, testRecord1.firstname, "names should be equal");
					test.equal(results.find.length, 2, "should find 2 records");
					test.equal(results.first.firstname, testRecord1.firstname, "names should be equal");
					
					db.delete(results.insert, function () {
						db.first(results.insert, function (err, result) {
							test.equal(result.record, void 0, "record must be deleted");
							cb();
						});
					});
				},
				function (cb) {
					var r = { firstname: c.first() };

					db.update(testRecord1, r, function (err, records) {
						test.equal(records.length, 1, "should have at least 1 record updated");
						cb();
					});
				}
			],
			function (err) {
				test.equal(db.is_open, true, "to_close should be true");
				test.equal(db.to_close, false, "to_close should be false");
				db.close(err, function () {
					test.equal(db.to_close, true, "to_close should be true");
					test.equal(db.is_open, false, "to_close should be true");
					test.done();
				});
			}
		);
	},
	"Can find total record": function (test) {
		var db = this.db;

		db.insert(testRecord1, function () {
			db.first({ firstname: testRecord1.firstname }, function (err, first) {
				db.first({ firstname: "blaaaah" }, function (err, zero) {
					test.equal(first.total, 2, "total record should be 2");
					test.equal(zero.total, 0, "zero should be 0");
					test.done();
				});
			});
		})
	},
	"Can replace record": function (test) {
		var r = { id : 1, firstname : c.first() };

		this.db.replace(r, function (err, record) {
			test.equal(record.firstname, r.firstname, "names should be equal");
			test.equal(record.id, r.id, "IDs should be equal");
			test.done();
		});
	},
	"Can find IN Order by field": function (test) {
		this.db.find({
			id : [3,1,2],
			$sort : true
		}, function (err, data) {
			var records = data.records;
			test.equal(records[0].id, 3, "first record id should be 3");
			test.equal(records[1].id, 1, "second record id should be 1");
			test.equal(records[2].id, 2, "third record id should be 2");
			test.done();
		});
	},
	"Can find IN Order by field with other wheres" : function (test) {
		this.db.find({
			id : [3,1,2],
			firstname : testRecord1.firstname
		}, function (err, data) {
			test.equal(data.records[0].id, 1, "id should be 1");
			test.done();
		});
	},
	"Can find where and $groups" : function (test) {
		this.db.find({
			$groups : [
				{ id : 99 },
				{ firstname : testRecord1.firstname }
			]
		}, function (err, data) {
			test.equal(data.records.length, 0, "id should be 1");
			test.done();
		});
	},
	"Can find where or $groups with order by fields" : function (test) {
		this.db.find({
			$or_groups : [
				{ id : [999, 999], $sort : true },
				{ id : [3,1]     , $sort : true }
			]
		}, function (err, data) {
			test.equal(data.records[0].id, 3, "id should be 3");
			test.equal(data.records[1].id, 1, "id should be 1");
			test.done();
		});
	},
	"Can find aggregate functions" : function (test) {
		this.db.first({
			id : { $max : "id" }
		}, function (err, data) {
			test.equal(data.record.id, 3, "id should be 3");
			test.done();
		});
	},
	"Can find aggregate functions with op" : function (test) {
		this.db.first({
			id : {
				$fn : {
					$max : "id",
					op   : "$is"
				}
			}
		}, function (err, data) {
			test.equal(data.record.id, 3, "id should be 3");
			test.done();
		});
	}
};
