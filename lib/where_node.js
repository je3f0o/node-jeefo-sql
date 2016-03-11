/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : where_node.js
* Purpose    :
* Created at : 2015-02-11
* Updated at : 2016-03-12
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var is_array = function (value) {
	return Array.isArray(value);
};

var is_function = function (value) {
	return typeof value === "function";
};

var is_object = function (value) {
	return typeof value === "object";
};

var OPERATORS = {
	$is   : "=",
	$not  : "<>",
	$like : "LIKE",
	$lte  : "<=",
	$gte  : ">=",
	$lt   : "<",
	$in   : "IN",
	$gt   : ">"
}, p;

var WhereNode = module.exports = function (node, table) {
	this.in      = {};
	this.table   = table;
	this.fields  = [];
	this.to_sort = node.$sort || false;

	delete node.$sort;

	Object.keys(node).forEach(function (key) {
		var value = node[key];

		if (is_object(value)) {
			if (is_array(value)) { // array of values, this means IN(,,,)
				this.in[key] = value;
			} else if (value === null) {
				this.fields.push({
					key     : key,
					is_null : true
				});
			} else { 
				this.set_ops(key, value); // the value is object, so it means go to see `set_ops` Func
			}
		} else {
			this.set_is(key, value)
		}
	}, this);

	return this;
}
var parse_table_name = WhereNode.parse_table_name = function (table) {
	return table.split(".").map(function (piece) {
		return "`" + piece + "`";
	}).join(".");
}
p = module.exports.prototype;

p.set_is = function (key, value) {
	this.fields.push({
		op    : "=",
		key   : key,
		value : value
	});

	return this;
};

p.set_aggregate = function (key, sub_query) {
	var fn = (sub_query.$fn.indexOf(".") > 0) ? ("`" + sub_query.$fn + "`") : sub_query.$fn,
		query = "SELECT " + fn + "(`" + sub_query.$arg + "`) FROM " + parse_table_name(sub_query.$table),
		op = "=";

	if (sub_query.$op) {
		op = OPERATORS[sub_query.$op];
		if (! op) {
			throw new Error("WhereNode - invalid operator: " + sub_query.$op);
		}
	}
	["$table", "$fn", "$op", "$arg"].forEach(function (prop) {
		delete sub_query[prop];
	});
	var child_node = new WhereNode(sub_query);
	child_node = child_node.build();

	if (child_node.str.length) {
		query += " WHERE " + child_node.str;
	}

	this.fields.push({
		op      : op,
		key     : key,
		value   : "(" + query + ")",
		$values : child_node.values,
		is_func : true
	});
};

p.set_ops = function (field, object) {
	Object.keys(object).forEach(function (key) {
		var value = object[key];

		key = key.toLowerCase();

		if (key === "$aggregate") {
			this.set_aggregate(field, value)
		} else if (key === "$not" && value === null) {
			this.fields.push({
				key         : field,
				is_not_null : true
			});
		} else if (OPERATORS[key]) {
			this.fields.push({
				op    : OPERATORS[key],
				key   : field,
				value : value
			});
		} else {
			throw new Error("WhereNode - invalid property : " + key);
		}

	}, this);
	
	return this;
};

p.build = function () {
	var query         = { str : "", values : [] },
		place_holders = [];
	
	this.fields.forEach(function (o) {
		if (o.is_func) {
			place_holders.push("`" + o.key + "` " + o.op + " " + o.value);
			query.values = query.values.concat(o.$values);
		} else if (o.is_null) {
			place_holders.push("`" + o.key + "` IS NULL");
		} else if (o.is_not_null) {
			place_holders.push("`" + o.key + "` IS NOT NULL");
		} else {
			place_holders.push("`" + o.key + "` " + o.op + " ?");
			query.values.push(o.value);
		}
	}, this);

	Object.keys(this.in).forEach(function (key) {
		var temp_place_holders = Array.apply(null, new Array(this.in[key].length))
			.map(String.prototype.valueOf, "?")
			.join(", ");

		place_holders.push("`" + key + "` IN (" + temp_place_holders + ")");

		query.values = query.values.concat(this.in[key]);

		if (this.to_sort) {
			query.sort = {
				key          : key,
				values       : this.in[key],
				place_holder : temp_place_holders
			};
		}
	}, this);

	if (place_holders.length) {
		query.str = "(" + place_holders.join(" AND ") + ")";
	}

	return query;
};
