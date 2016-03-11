/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : jeefo_sql.js
* Purpose    :
* Created at : 2014-11-08
* Updated at : 2015-07-30
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var JeefoDB = require("./jeefo_sql_ex");

module.exports = function (config) {
	var use_pool_cache = config ? false : true;

	return new JeefoDB(config, use_pool_cache);
};
