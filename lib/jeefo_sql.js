/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : jeefo_sql.js
* Purpose    :
* Created at : 2014-11-08
* Updated at : 2016-06-15
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var path            = require("path"),
	async           = require("async"),
	mysql_pool      = require("./mysql_pool"),
	SqlQueryBuilder = require("./sql_query_builder"),
	config_path     = path.join(process.cwd(), "config"),
	pooler_cache, config_cache, p;

function check_valid_where_in (wheres) {
	var is_valid = true;

	Object.keys(wheres).forEach(function (key) {
		var value = wheres[key];
		if (value instanceof Array && value.length === 0) {
			is_valid = false;
		}
	})
	
	return is_valid;
}

var clone = function (object) {
	return JSON.parse(JSON.stringify(object));
};

module.exports = function (config, use_pool_cache) {
	if (! config) {
		config = require(config_path).database;
	}

	this.adapter = config.adapter;

	switch (this.adapter) {
		case "mysql" :
			this.config = {};

			Object.keys(config).forEach(function (prop) {
				if (prop !== "adapter") {
					this.config[prop] = config[prop];
				}
			}, this);

			this.pool = mysql_pool(this.config);

			break;
		case "sqlite3" :
			this.db_name = config.db_name;
			break;
		default :
			throw new Error("Invalid database!");
	}

	this.query_builder = new SqlQueryBuilder();
	return this;
}

var next = module.exports.next = function (err, results, last_query, callback) {
	if (err) {
		console.error("jeefo-sql ERROR at next :", err, last_query);
	}
	callback(err, results, last_query);
}

p = module.exports.prototype;

p.open = function (callback) {
	this.pool(function (err, mysql_conn) {
		if (! err && ! mysql_conn.__event_added) {
			mysql_conn.on("error", function(err) {      
				console.error("CONNECTION ERROR IN POOLER");
				mysql_conn.release();
			});
			mysql_conn.__event_added = true;
		}
		callback(err, mysql_conn);
	}.bind(this));
};

p.no_timestamp = function () {
	this.query_builder.has_created_time = false;
	this.query_builder.has_updated_time = false;
	return this;
};

p.no_updated_time = function () {
	this.query_builder.has_updated_time = false;
	return this;
};

p.insert = function (table, data, callback) {
	var qb = this.query_builder,
		$this = this,
		mysql_conn, last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				last_query = qb.build_insert_query(table, data);
				mysql_conn.query(last_query.str, last_query.values, cb);
			},
			function (results, fields, cb) {
				$this.first(table, { id : results.insertId }, cb);
			},
			function (result, _last_query, cb) {
				cb(null, result.record);
			}
		],
		function (err, record) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, record, last_query, callback);
		}
	);
};

p.find = function (table, wheres, callback) {
	var $this  = this,
		result = {
			records : [],
			total   : 0
		}, mysql_conn, last_query;

	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					mysql_conn.query(last_query.str, last_query.values, cb);
				} else {
					cb(null, [], null); // empty result
				}
			},
			function (data, fields, cb) {
				result.records = data;
				if (data.length > 0) {
					mysql_conn.query(last_query.total, last_query.total_values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (data, fields, cb) {
				if (result.records.length > 0) {
					result.total = data[0].total;
				}
				cb();
			}
		],
		function (err) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, result, last_query, callback);
		}
	);
};

p.total = function (table, wheres, callback) {
	var $this = this,
		total = 0,
		mysql_conn, last_query;
	
	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					mysql_conn.query(last_query.total, last_query.total_values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (record, fields, cb) {
				if (record.length === 1) {
					total = record[0].total;
				}
				cb();
			}
		],
		function (err) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, total, last_query, callback);
		}
	);
}

p.first = function (table, wheres, callback) {
	var $this  = this,
		result = {
			record : null,
			total  : 0
		}, mysql_conn, last_query;

	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var qb = $this.query_builder;
				
				last_query = qb.reset().where(wheres).limit(1).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					mysql_conn.query(last_query.str, last_query.values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (data, fields, cb) {
				if (data.length > 0) {
					result.record = data[0];
					mysql_conn.query(last_query.total, last_query.total_values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (data, fields, cb) {
				if (data.length === 1) {
					result.total = data[0].total;
				}
				cb();
			}
		],
		function (err) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, result, last_query, callback);
		}
	);
};

p.last = function (table, wheres, callback) {
	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	if (wheres.$order_by === void 0) {
		wheres.$order_by = {};
	}

	wheres.$order_by.id = "desc";
	this.first(table, wheres, callback);
};

p.update = function(table, wheres, fields, callback) {
	var $this = this,
		qb    = this.query_builder,
		where_copy = clone(wheres),
		mysql_conn, last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var old_has_updated_time = qb.has_updated_time;

				qb.reset().where(wheres);
				qb.has_updated_time = old_has_updated_time;

				last_query = qb.build_update_query(table, fields);
				mysql_conn.query(last_query.str, last_query.values, cb);
			},
			function (results, f, cb) {
				$this.find(table, where_copy, cb);
			},
			function (result, last_query, cb) {
				cb(null, result.records);
			}
		],
		function (err, records) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, records, last_query, callback);
		}
	);
};

p.delete = function (table, wheres, callback) {
	var $this = this,
		mysql_conn, last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_delete_query(table);
				mysql_conn.query(last_query.str, last_query.values, cb);
			}
		],
		function (err, records) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, records, last_query, callback);
		}
	);	
};

p.exec = function (query, values, callback) {
	var last_query = { str : query }, mysql_conn;

	if (typeof values === "function") {
		callback = values;
		values = void 0;
	} else {
		last_query.values = values;
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				mysql_conn.query(last_query.str, last_query.values, cb);
			}.bind(this)
		],
		function (err, results) {
			if ( mysql_conn ) {
				mysql_conn.release();
			}
			next(err, results, last_query, callback);
		}.bind(this)
	);
};
