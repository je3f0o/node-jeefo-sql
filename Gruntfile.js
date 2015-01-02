/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : Gruntfile.js
* Purpose    :
* Created at : 2014-11-15
* Updated at : 2014-11-15
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

module.exports = function (grunt) {

	var config = {
		"nodeunit" : {
			files : ["test/**/*_test.js"]
		}
	};
	
	grunt.initConfig(config);

	grunt.loadNpmTasks("grunt-contrib-nodeunit");

	grunt.registerTask("default", ["nodeunit"]);
};
