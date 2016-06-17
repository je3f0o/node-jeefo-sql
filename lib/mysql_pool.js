/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : mysql_pool.js
* Purpose    :
* Created at : 2014-11-07
* Updated at : 2015-07-22
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var mysql = require("mysql");

module.exports = function (config) {

	 var pooler = mysql.createPool({
		connectionLimit    : config.connection_limit || 10, //important
		host               : config.host     || "127.0.0.1",
		user               : config.user     || "root",
		password           : config.password || "",
		database           : config.db_name,
		multipleStatements : config.multi_statements || false,
		dateStrings        : true
	});

	return pooler.getConnection.bind(pooler);
};

