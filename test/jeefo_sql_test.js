"use strict";

var async   = require("async"),
	mysql   = require("mysql"),
	moment  = require("moment"),
	Chance  = require("chance"),
	jeefoDB = require("../lib/main"),

	test_db_config = {
		adapter          : "mysql",
		host             : "127.0.0.1",
		user             : "root",
		password         : "toor",
		db_name          : "test",
		dataStrings      : true,
		multi_statements : true
	},

	test_table  = "users",
	date_format = "YYYY-MM-DD",

	c = new Chance(),
	testRecord1 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) },
	testRecord2 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) },
	testRecord3 = { firstname: c.first(), lastname: c.last(), age: c.age(), birthday: moment(c.birthday()).format(date_format) };

function operator_test(record, query, callback) {
	var mysql_conn = jeefoDB(test_db_config);
	mysql_conn.insert(test_table, record, function () {
		mysql_conn.find(test_table, query, callback);
	});
}

exports["jeefo-sql"] = {
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
				this.db = jeefoDB(test_db_config);
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
	"Can create mysql_conn instance": function (test) {
		var mysql_conn;
		test.doesNotThrow(function () {
			mysql_conn = jeefoDB(test_db_config);
		});
		test.notEqual(mysql_conn, void 0, "should be instance of JeefoDB");
		test.done();
	},
	"Can directly execute sql query" : function (test) {
		this.db.exec("SELECT 3 * 3 AS total;", function (err, results) {
			test.equal(results[0].total, 9, "3 times 3 is should be 9");
			test.done();
		});
	},
	"Can directly execute sql query with values" : function (test) {
		this.db.exec("SELECT ? * ? AS total;", [5, 5], function (err, results) {
			test.equal(results[0].total, 25, "5 times 5 is should be 25");
			test.done();
		});
	},
	"Can insert record": function (test) {
		this.db.insert(test_table, testRecord1, function (err, record) {
			test.equal(record.id, 4, "id should be 4");
			test.equal(testRecord1.firstname, record.firstname, "names should be equal");
			test.done();
		});
	},
	"Can insert record with NULL": function (test) {
		var clone = JSON.parse(JSON.stringify(testRecord1));
		clone.firstname = null;

		this.db.insert(test_table, clone, function (err, record) {
			test.equal(record.id, 4, "id should be 4");
			test.equal(record.firstname, null, "firstname should be NULL");
			test.done();
		});
	},
	"Can find records by one key": function (test) {
		this.db.find(test_table, { firstname: testRecord1.firstname }, function (err, result) {
			test.equal(result.records.length, 1, "should find 1 record");
			test.equal(testRecord1.firstname, result.records[0].firstname, "names should be equal");
			test.done();
		});
	},
	"Can find records by multiple keys": function (test) {
		this.db.find(test_table, { firstname: testRecord1.firstname, age: testRecord1.age }, function (err, result) {
			test.equal(result.records.length, 1, "should find 1 record");
			test.equal(testRecord1.firstname, result.records[0].firstname, "names should be equal");
			test.equal(testRecord1.age, result.records[0].age, "ages should be equal");
			test.done();
		});
	},
	"Can find multiple records": function (test) {
		var db = this.db,
			r = { firstname: testRecord1.firstname, age: c.age() };

		db.insert(test_table, r, function () {
			db.find(test_table, { firstname: testRecord1.firstname }, function (err, result) {
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

			db.insert(test_table, testRecord1, function () {
				db.find(test_table, {
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

			db.insert(test_table, testRecord1, function () {
				db.find(test_table, { $limit : 2 }, function (err, result) {
					test.equal(result.records[0].id, 1, "first record.id should be 1");
					test.equal(result.records.length, 2, "records.length should be 2");
					test.done();
				});
			});
		},
		"$order_by" : function (test) {
			var db = this.db;

			db.first(test_table, {
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

			db.insert(test_table, record, function () {
				db.first(test_table, {
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
		this.db.first(test_table, function (err, result) {
			test.equal(result.record.id, 1, "record.id should be 1");
			test.done();
		});
	},
	"Can get a last record": function (test) {
		this.db.last(test_table, function (err, result) {
			test.equal(result.record.id, 3, "record.id should be 3");
			test.done();
		});
	},
	"Can find first record": function (test) {
		this.db.first(test_table, { firstname: testRecord1.firstname }, function (err, result) {
			test.equal(result.record.firstname, testRecord1.firstname, "names should be equal");
			test.done();
		});
	},
	"Can find all records": function (test) {
		this.db.find(test_table, function (err, result) {
			test.equal(result.records.length, 3, "should find 3 records");
			test.done();
		});
	},
	"Can find IN method records": function (test) {
		this.db.find(test_table, { id : [1, 2] }, function (err, result) {
			test.equal(result.records.length, 2, "should find 2 records");
			test.done();
		});
	},
	"Can find IN method empty array": function (test) {
		this.db.find(test_table, { id : [] }, function (err, result) {
			test.equal(result.records.length, 0, "should find 0 records");
			test.equal(result.total, 0, "should find 0 records");
			test.done();
		});
	},
	"Can get total results number": function (test) {
		this.db.total(test_table, { firstname : testRecord1.firstname }, function (err, total) {
			test.equal(total, 1, "should at least 1 records");
			test.done();
		});
	},
	"Can update records": function (test) {
		this.db.update(test_table, { firstname: testRecord1.firstname }, { age: 200 }, function (err, records) {
			test.notEqual(records.length, 0, "should have at least 1 record updated");
			records.forEach(function (record) {
				test.equal(record.age, 200, "age should be updated");
			});
			test.done();
		});
	},
	"Can delete records": function (test) {
		var db = this.db;
		
		db.delete(test_table, { firstname: testRecord1.firstname }, function () {
			db.find(test_table, { firstname: testRecord1.firstname }, function (err, result) {
				test.equal(result.records.length, 0, "no records should be found");
				test.done();
			});
		});
	},
	"Can find total record": function (test) {
		var db = this.db;

		db.insert(test_table, testRecord1, function () {
			db.first(test_table, { firstname: testRecord1.firstname }, function (err, first) {
				db.first(test_table, { firstname: "blaaaah" }, function (err, zero) {
					test.equal(first.total, 2, "total record should be 2");
					test.equal(zero.total, 0, "zero should be 0");
					test.done();
				});
			});
		})
	},
	"Can find IN Order by field": function (test) {
		this.db.find(test_table, {
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
		this.db.find(test_table, {
			id : [3,1,2],
			firstname : testRecord1.firstname
		}, function (err, data) {
			test.equal(data.records[0].id, 1, "id should be 1");
			test.done();
		});
	},
	"Can find where and $groups" : function (test) {
		this.db.find(test_table, {
			$groups : [
				{ id        : 99 },
				{ firstname : testRecord1.firstname }
			]
		}, function (err, data) {
			test.equal(data.records.length, 0, "id should be 1");
			test.done();
		});
	},
	"Can find where or $groups with order by fields" : function (test) {
		this.db.find(test_table, {
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
	"Can find aggregate function with op" : function (test) {
		this.db.first(test_table, {
			id : {
				$aggregate : {
					$fn    : "MAX",
					$op    : "$is",
					$arg   : "id",
					$table : test_table
				}
			}
		}, function (err, data) {
			test.equal(data.record.id, 3, "id should be 3");
			test.done();
		});
	},
	"Can find aggregate function distinct with where fields" : function (test) {
		this.db.find(test_table, {
			id : {
				$aggregate : {
					$fn    : "DISTINCT",
					$op    : "$in",
					$arg   : "id",
					$table : test_table,
					id     : { $gt : 1 }
				}
			},
			$limit    : 2,
			$order_by : { id : "desc" }
		}, function (err, data) {
			test.equal(data.records.length, 2, "data length should be 2");
			test.equal(data.records[0].id, 3, "first record id should be 3");
			test.equal(data.records[1].id, 2, "second record id should be 2");
			test.done();
		});
	},
	"Can multiple select fields and count" : function (test) {
		this.db.find(test_table, {
			$select   : [ "id", "firstname", "lastname" ],
			$limit    : 2,
			$order_by : { id : "desc" }
		}, function (err, data) {
			test.equal(data.records.length, 2, "data length should be 2");
			test.equal(data.records[0].id, 3, "first record id should be 3");
			test.equal(data.records[1].id, 2, "second record id should be 2");
			test.equal(data.total, 3, "total records should be 3");
			test.done();
		});
	},
	"Set database and table name" : function (test) {
		this.db.find("information_schema.columns", {
			COLUMN_NAME : "CHARACTER_SET_NAME",
			TABLE_NAME  : "CHARACTER_SETS"
		}, function (err, data) {
			test.equal(data.records[0].TABLE_SCHEMA, "information_schema", "TABLE_SCHEMA should be 'information_schema'");
			test.equal(data.total, 1, "Total records should be 1");
			test.done();
		});
	},
	"Set filter table data" : function (test) {
		this.db.filter(test_table, {
			firstname    : testRecord1.firstname,
			lastname     : testRecord1.lastname,
			nonExistentKey : "UNUSED"
		}, function (err, data) {
			test.equal(Object.keys(data).length, 2, "Final data object properties length should be 2");
			test.equal(data.firstname, testRecord1.firstname, "firstname should be " + testRecord1.firstname);
			test.equal(data.firstname, testRecord1.firstname, "firstname should be " + testRecord1.firstname);
			test.equal(data.lastname, testRecord1.lastname, "lastname should be " + testRecord1.lastname);
			test.equal(data.nonExistentKey, undefined, "nonExistentKey should be undefined");
			test.done();
		});
	},
	"Update with NULL" : function (test) {
		this.db.update(test_table, {
			$groups : [
				{ id : 1 },
				{ id : { $not : null } }
			]
		}, {
			firstname : "jeefo"
		}, function (err, records) {
			test.equal(records.length, 1, "updated records length should be 1");
			test.equal(records[0].firstname, "jeefo", "updated record's firstname should be 'jeefo'");
			test.done();
		});
	},
	"Can filter invalid $limit max value" : function (test) {
		this.db.find(test_table, { $limit : -1 }, function (err, result) {
			test.equal(result.total, 0, "total result should be zero");
			test.done();
		});
	},
	"Can filter invalid $limit object value" : function (test) {
		this.db.find(test_table, { $limit : { offset : -1, max : -1 } }, function (err, result) {
			test.equal(result.total, 0, "total result should be zero");
			test.done();
		});
	}
};
