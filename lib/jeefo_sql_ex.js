/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : jeefo-sql-ex.js
* Purpose    :
* Created at : 2015-07-30
* Updated at : 2015-07-30
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var async     = require("async"),
	JeefoDB   = require("./jeefo_sql"),
	prototype = JeefoDB.prototype;

prototype.filter = function (table, data, callback) {
	var	keys  = Object.keys(data),
		$this = this,
		mysql_conn, last_query;
	
	async.waterfall(
		[
			this.open.bind(this),
			function (conn, cb) {
				mysql_conn = conn;
				var error = keys.length === 0 ? "EMPTY" : null;
				cb(error);
			},
			function (cb) {
				$this.find("information_schema.columns", {
					TABLE_NAME   : table,
					COLUMN_NAME  : keys,
					TABLE_SCHEMA : $this.config.db_name,
					$select      : "COLUMN_NAME"
				}, cb);
			},
			function (result, _last_query, cb) {
				var valid_keys = result.records.filter(function (row) {
					return keys.indexOf(row.COLUMN_NAME) >= 0;
				}).map(function (row) {
					return row.COLUMN_NAME;
				});

				keys.forEach(function (key) {
					if (valid_keys.indexOf(key) < 0) { delete data[key]; }
				});
				
				last_query = _last_query;
				cb(null, data);
			}
		],
		function (err) {
			if (err === "EMPTY") {
				err  = null;
				data = {};
			}

			if ( mysql_conn ) {
				mysql_conn.release();
			}

			JeefoDB.next(err, data, last_query, callback);
		}
	);
};

module.exports = JeefoDB;
