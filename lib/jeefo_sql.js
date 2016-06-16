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
	extend          = require("util")._extend,
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
	if (! config_cache) {
		config_cache = require(config_path).database;
	}

	if (! config) { config = config_cache; }

	this.adapter = config.adapter;

	switch (this.adapter) {
		case "mysql" :
			if (use_pool_cache) {
				if (pooler_cache) {
					this.pool   = pooler_cache;
					this.config = config_cache;
				} else {
					this.config = {};

					Object.keys(config).forEach(function (prop) {
						if (prop !== "adapter") {
							this.config[prop] = config[prop];
						}
					}, this);

					this.pool = pooler_cache = mysql_pool(this.config);
				}
			} else {
				this.config = {};

				Object.keys(config).forEach(function (prop) {
					if (prop !== "adapter") {
						this.config[prop] = config[prop];
					}
				}, this);

				Object.keys(config_cache).forEach(function (prop) {
					if (prop !== "adapter" && this.config[prop] === void 0) {
						this.config[prop] = config_cache[prop];
					}
				}, this);

				this.pool = mysql_pool(this.config);
			}

			this.is_open = false;
			break;
		case "sqlite3" :
			this.db_name = config.db_name;
			break;
		default :
			throw new Error("Invalid database!");
	}

	this.to_close = true;
	this.query_builder = new SqlQueryBuilder();
	return this;
}
var next = module.exports.next = function (err, results, last_query, callback) {
	if (err) {
		console.error("jeefo-sql ERROR at next :", last_query);
	}
	callback(err, results, last_query);
}

p = module.exports.prototype;

p.open = function (to_close, callback) {
	if (callback === void 0) {
		callback = to_close;
	} else {
		this.to_close = to_close;
	}

	if (this.is_open) {
		callback();
	} else {
		this.pool.pool(function (err, mysql_conn) {
			if (! err) {
				this.db      = mysql_conn;
				this.is_open = true;
			}
			callback(err);
		}.bind(this));
	}
};

p.close = function (err, results, last_query, callback) {
	if (err) {
		console.error("jeefo-sql ERROR at close :", last_query);
		return callback(err);
	}

	if (typeof last_query === "function") {
		callback   = last_query;
		last_query = null;
	} else if (typeof results === "function") {
		callback = results;
		results = last_query = null;
	}

	this.to_close = true;

	if (this.db && this.is_open) {
		this.is_open = false;
		this.pool.release(this.db);
	}

	if (callback) { callback(err, results, last_query); }
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
		last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				last_query = qb.build_insert_query(table, data);
				$this.db.query(last_query.str, last_query.values, cb);
			},
			function (results, fields, cb) {
				$this.first(table, { id : results.insertId }, cb);
			},
			function (result, last_query, cb) {
				cb(null, result.record);
			}
		],
		function (err, record) {
			if ($this.to_close) { $this.close(err, record, last_query, callback); }
			else if (callback)  { next(err, record, last_query, callback); }
		}
	);
};

p.find = function (table, wheres, callback) {
	var $this  = this,
		result = {
			records : [],
			total   : 0
		}, last_query;

	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					$this.db.query(last_query.str, last_query.values, cb);
				} else {
					cb(null, [], null); // empty result
				}
			},
			function (data, fields, cb) {
				result.records = data;
				if (data.length > 0) {
					$this.db.query(last_query.total, last_query.total_values, cb);
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
			if ($this.to_close) { $this.close(err, result, last_query, callback); }
			else if (callback)  { next(err, result, last_query, callback); }
		}
	);
};

p.total = function (table, wheres, callback) {
	var $this = this,
		total = 0,
		last_query;
	
	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					$this.db.query(last_query.total, last_query.total_values, cb);
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
			if ($this.to_close) { $this.close(err, total, last_query, callback); }
			else                { next(err, total, last_query, callback);        }
		}
	);
}

p.first = function (table, wheres, callback) {
	var $this  = this,
		result = {
			record : null,
			total  : 0
		}, last_query;

	if (typeof wheres === "function") {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				var qb = $this.query_builder;
				
				last_query = qb.reset().where(wheres).limit(1).build_select_query(table);
				if (check_valid_where_in(wheres)) {
					$this.db.query(last_query.str, last_query.values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (data, fields, cb) {
				if (data.length > 0) {
					result.record = data[0];
					$this.db.query(last_query.total, last_query.total_values, cb);
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
			if ($this.to_close) { $this.close(err, result, last_query, callback); }
			else                { next(err, result, last_query, callback);        }
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
		last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				var old_has_updated_time = qb.has_updated_time;

				qb.reset().where(wheres);
				qb.has_updated_time = old_has_updated_time;

				last_query = qb.build_update_query(table, fields);
				$this.db.query(last_query.str, last_query.values, cb);
			},
			function (results, f, cb) {
				$this.find(table, where_copy, cb);
			},
			function (result, last_query, cb) {
				cb(null, result.records);
			}
		],
		function (err, records) {
			if ($this.to_close) { $this.close(err, records, last_query, callback); }
			else if (callback)  { next(err, records, last_query, callback);        }
		}
	);
};

p.delete = function (table, wheres, callback) {
	var $this = this,
		last_query;

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				var qb = $this.query_builder;

				last_query = qb.reset().where(wheres).build_delete_query(table);
				$this.db.query(last_query.str, last_query.values, cb);
			}
		],
		function (err, records) {
			if ($this.to_close) { $this.close(err, records, last_query, callback); }
			else if (callback)  { next(err, records, last_query, callback);        }
		}
	);	
};

p.exec = function (query, values, callback) {
	var last_query = { str : query };

	if (typeof values === "function") {
		callback = values;
		values = void 0;
	} else {
		last_query.values = values;
	}

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				this.db.query(last_query.str, last_query.values, cb);
			}.bind(this)
		],
		function (err, results) {
			if (this.to_close) { this.close(err, results, last_query, callback); }
			else               { next(err, results, last_query, callback);       }
		}.bind(this)
	);
};
