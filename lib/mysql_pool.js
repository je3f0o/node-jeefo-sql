/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : mysql_pool.js
* Purpose    :
* Created at : 2014-11-07
* Updated at : 2014-12-31
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var mysql = require("mysql"),
	pool  = require("generic-pool"),
	p;

function MysqlPool (pool) {
	this.pooler = pool;
}
p = MysqlPool.prototype;

p.pool = function (callback) {
	this.pooler.acquire(callback);
};

p.release = function (mysql_conn) {
	this.pooler.release(mysql_conn, function (err, z) {
		console.log(err, z)
	});
};

module.exports = function (config) {

	var mysql_pool = pool.Pool({
		name   : "mysql",
		create : function (cb) {
			var instance = mysql.createConnection({
				host     : config.host     || "127.0.0.1",
				user     : config.user     || "root",
				password : config.password || "",
				database : config.db_name,
				multipleStatements : true,
				dataStrings : true
			});

			cb(null, instance);
		},
		destroy : function (mysql_conn) { mysql_conn.end(); },
		max               : config.pooled_connections  || 100,
		idleTimeoutMillis : config.idle_timeout_millis || 3000,
		log               : config.log || false
	});

	return new MysqlPool(mysql_pool);
};

