/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : jeefo_sql.js
* Purpose    :
* Created at : 2014-11-08
* Updated at : 2015-07-22
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var path            = require("path"),
	async           = require("async"),
	mysql_pool      = require("./mysql_pool"),
	SqlQueryBuilder = require("./sql_query_builder"),
	config_path     = path.join(process.cwd(), "config"),
	pooler_cache, config_cache, p;

function next (err, results, callback, $this) {
	if (err) {
		console.error("jeefo-sql ERROR :", $this.last_query);
		throw new Error(err);
	} else if (callback) {
		callback(null, results);
	}
}

function check_valid_where_in (wheres) {
	var keys = Object.keys(wheres),
		i = 0, len = keys.length,
		is_valid = true,
		key;
	
	for (; i < len; ++i) {
		key = keys[i];
		if (wheres[key] instanceof Array && wheres[key].length === 0) {
			is_valid = false;
			break;
		}
	}
	
	return is_valid;
}

function JeefoDB (config, table, use_cache) {
	this.adapter    = config.adapter;
	this.last_query = null;

	switch (this.adapter) {
		case "mysql" :
			if (use_cache) {
				if (pooler_cache) {
					this.pool = pooler_cache;
				} else {
					if (! this.config) {
						this.config = {};

						Object.keys(config).forEach(function (prop) {
							if (prop !== "adapter") {
								this.config[prop] = config[prop];
							}
						}, this);
					}
					this.pool = pooler_cache = mysql_pool(this.config);
				}
			} else {
				this.pool = mysql_pool(config);
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
	this.query_builder = new SqlQueryBuilder(table);
	return this;
}
p = JeefoDB.prototype;

p.open = function (to_close, callback) {
	if (callback === void 0) {
		callback = to_close;
	} else {
		this.to_close = to_close;
	}

	if (! this.is_open) {
		this.pool.pool(function (err, mysql_conn) {
			if (! err) {
				this.is_open = true;
				this.db      = mysql_conn;
			}
			callback(err);
		}.bind(this));
	} else {
		callback()
	}
};

p.close = function (err, results, callback) {
	if (err) {
		console.error("jeefo-sql ERROR :", this.last_query);
		throw new Error(err);
	} else {
		var call_cb = function () {
			if (callback === void 0) {
				callback = results;

				if (callback) { callback(); }
			} else if (callback) {
				callback(null, results);
			}
		};

		this.to_close = true;

		if (this.db && this.is_open) {
			this.is_open = false;
			this.pool.release(this.db);
		}

		call_cb();
	}
};

p.set_table = function (table) {
	this.query_builder.table = table.split(".").map( function (name) {
		return "`" + name + "`";
	}).join(".");
	return this;
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

p.insert = function (record, callback) {
	var qb = this.query_builder,
		$this = this;

	qb.set_fields(record);

	async.waterfall(
		[
			this.open.bind(this),
			function (cb) {
				$this.last_query = qb.build_insert_query();
				$this.db.query($this.last_query.str, $this.last_query.values, cb);
			},
			function (results, fields, cb) {
				$this.first({ id : results.insertId }, cb);
			},
			function (result, cb) {
				cb(null, result.record);
			}
		],
		function (err, record) {
			if ($this.to_close) { $this.close(err, record, callback); }
			else if (callback)  { next(err, record, callback, $this); }
		}
	);
};

p.find = function (wheres, callback) {
	var $this  = this,
		result = {
			records : [],
			total   : 0
		};

	if (callback === void 0) {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// find method
			function (cb) {
				var qb = $this.query_builder;

				if (check_valid_where_in(wheres, cb)) {
					$this.last_query = qb.reset().where(wheres).build_select_query();
					$this.db.query($this.last_query.str, $this.last_query.values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (records, fields, cb) {
				result.records = records;
				if (records.length > 0) {
					$this.db.query($this.last_query.total, $this.last_query.total_values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (record, fields, cb) {
				if (result.records.length > 0) {
					result.total = record[0].total;
				}
				cb();
			}
		],
		// Close the db and return results.
		function (err) {
			if ($this.to_close) { $this.close(err, result, callback); }
			else if (callback)  { next(err, result, callback, $this); }
		}
	);
};

p.total = function (wheres, callback) {
	var $this = this,
		total = 0;
	
	if (callback === void 0) {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// first method
			function (cb) {
				var qb = $this.query_builder;
				

				if (check_valid_where_in(wheres, cb)) {
					$this.last_query = qb.reset().where(wheres).limit(1).build_select_query();
					$this.db.query($this.last_query.total, $this.last_query.total_values, cb);
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
		// Close the db and return results.
		function (err) {
			if ($this.to_close) { $this.close(err, total, callback); }
			else                { next(err, total, callback, $this); }
		}
	);
}

p.first = function (wheres, callback) {
	var $this  = this,
		result = {
			record : null,
			total  : 0
		};

	if (callback === void 0) {
		callback = wheres;
		wheres   = {};
	}

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// first method
			function (cb) {
				var qb = $this.query_builder;
				
				if (check_valid_where_in(wheres, cb)) {
					$this.last_query = qb.reset().where(wheres).limit(1).build_select_query();
					$this.db.query($this.last_query.str, $this.last_query.values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (record, fields, cb) {
				if (record.length > 0) {
					result.record = record[0];
					$this.db.query($this.last_query.total, $this.last_query.total_values, cb);
				} else {
					cb(null, [], null);
				}
			},
			function (record, fields, cb) {
				if (record.length === 1) {
					result.total = record[0].total;
				}
				cb();
			}
		],
		// Close the db and return results.
		function (err) {
			if ($this.to_close) { $this.close(err, result, callback); }
			else                { next(err, result, callback, $this); }
		}
	);
};

p.last = function (wheres, callback) {
	if (callback === void 0) {
		callback = wheres;
		wheres   = {};
	}

	if (wheres.$order_by === void 0) {
		wheres.$order_by = {};
	}

	wheres.$order_by.id = "desc";
	this.first(wheres, callback);
};

p.update = function(wheres, fields, callback) {
	var $this = this,
		qb    = this.query_builder;

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// update method
			function (cb) {
				var has_updated_time = qb.has_updated_time;

				qb.reset().where(wheres).set_fields(fields);
				qb.has_updated_time = has_updated_time;

				$this.last_query = qb.build_update_query();
				$this.db.query($this.last_query.str, $this.last_query.values, cb);
			},
			// fetch inserted data
			function (results, f, cb) {
				$this.find(qb.fields, cb);
			},
			function (result, cb) {
				cb(null, result.records);
			}
		],
		// Close the db and return results.
		function (err, records) {
			if ($this.to_close) { $this.close(err, records, callback); }
			else if (callback)  { next(err, records, callback, $this); }
		}
	);
};

p.delete = function (wheres, callback) {
	var $this = this;

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// delete method
			function (cb) {
				var qb = $this.query_builder;

				$this.last_query = qb.reset().where(wheres).build_delete_query();
				$this.db.query($this.last_query.str, $this.last_query.values, cb);
			}
		],
		// Close the db and return results.
		function (err, records) {
			if ($this.to_close) { $this.close(err, records, callback); }
			else if (callback)  { next(err, records, callback, $this); }
		}
	);	
};

p.replace = function (record, callback) {
	var $this = this,
		qb    = this.query_builder;

	qb.set_fields(record);

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			// replace method
			function (cb) {
				$this.last_query = qb.build_replace_query();
				$this.db.query($this.last_query.str, $this.last_query.values, cb);
			},
			// fetch replaced record
			function (results, fields, cb) {
				$this.first(qb.fields, cb);
			},
			function (result, cb) {
				cb(null, result.record);
			}
		],
		// Close the db and return results.
		function (err, record) {
			if ($this.to_close) { $this.close(err, record, callback); }
			else if (callback)  { next(err, record, callback, $this); }
		}
	);
};

p.exec = function (query, values, callback) {
	callback = callback || values;
	values   = values instanceof Array ? values : [];

	this.last_query = {
		str    : query,
		values : values
	};

	async.waterfall(
		[
			// Get a connection.
			this.open.bind(this),
			function (cb) {
				this.db.query(this.last_query.str, this.last_query.values, cb);
			}.bind(this)
		],
		// Close the db and return results.
		function (err, results) {
			if (callback) { next(err, results, callback, this); }
		}.bind(this)
	);
};

module.exports = function (config, table, use_cache) {
	use_cache = use_cache || false;
	if (typeof config === "string") {
		use_cache = true;
		table = config;
		if (config_cache) {
			config = config_cache;
		} else {
			config = config_cache = require(config_path).database;
		}
	}

	return new JeefoDB(config, table, use_cache);
};
