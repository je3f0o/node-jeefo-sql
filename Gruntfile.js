/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : Gruntfile.js
* Purpose    :
* Created at : 2014-11-15
* Updated at : 2015-02-11
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

module.exports = function (grunt) {

	var config = {
		"nodeunit" : {
			"jeefo-sql" : ["test/jeefo_sql_test.js"],
			"where" : {
				src : ["test/where_test.js"]
			}
		}
	};
	
	grunt.initConfig(config);

	grunt.loadNpmTasks("grunt-contrib-nodeunit");

	grunt.registerTask("where", ["nodeunit:where"]);
	grunt.registerTask("default", ["nodeunit:jeefo-sql"]);
};
