/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : where_query.js
* Purpose    : Where tree
* Created at : 2015-02-11
* Updated at : 2015-02-12
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var where_obj = {
	$groups : [
		{
			is : {
				firstname : "john"
			},
			not : {
				lastname : "doe"
			}
		},
		{
			$or : {
				firstname : {
					$like : "%john%"
				},
				lastname : "doe"
			}
		},
		{
			id : 99
		},
		{
			$or : {
				id : [1,2,3,4]
			}
		}
	],
	$or_groups : [
	]
};

var WhereNode = require("./where_node"),
	p;

function Where () {
	this.groups = [];
	this.or_groups = [];
}
p = Where.prototype;

p.build = function () {
	var query = null;
	
	this.groups.forEach(function (group) {
		var result = group.build();

		if (query) {
			query.str += " AND " + result.str;
			query.values.concat(result.values);
		} else {
			query = result;
		}
	});

	this.or_groups.forEach(function (group) {
		var result = group.build();

		if (query) {
			query.str += " OR " + result.str;
			query.values.concat(result.values);
		} else {
			query = result;
		}
	});

	return query;
};

if (require.main === module) {
	var main_where       = new Where(),
		main_where_query = main_where.build();

	console.log(main_where_query);
}
