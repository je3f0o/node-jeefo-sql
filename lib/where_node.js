/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : where_node.js
* Purpose    :
* Created at : 2015-02-11
* Updated at : 2015-04-24
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

var OPERATORS = {
		$is   : "=",
		$not  : "<>",
		$like : "LIKE",
		$lte  : "<=",
		$gte  : ">=",
		$lt   : "<",
		$gt   : ">"
	},
	FUNCS = {
		"$min"      : function (field, table) { return "(SELECT MIN(`" + field + "`) FROM `" + table + "`)"; },
		"$max"      : function (field, table) { return "(SELECT MAX(`" + field + "`) FROM `" + table + "`)"; },
		"$avg"      : function (field, table) { return "(SELECT AVG(`" + field + "`) FROM `" + table + "`)"; },
		"$sum"      : function (field, table) { return "(SELECT SUM(`" + field + "`) FROM `" + table + "`)"; },
		"$distinct" : function (field, table) { return "(SELECT DISTINCT(`" + field + "`) FROM `" + table + "`)"; }
	},
	WhereNode, p;

module.exports = WhereNode = function (node, table) {
	this.in      = {};
	this.table   = table;
	this.fields  = [];
	this.to_sort = node.$sort || false;

	delete node.$sort;

	Object.keys(node).forEach(function (key) {
		var value = node[key];

		if (typeof(value) === "object") {
			if (value instanceof Array) {
				this.in[key] = value;
			} else if (value === null) {
				this.set_is(key, null)
			} else { 
				this.set_ops(key, value);
			}
		} else {
			this.set_is(key, value)
		}
	}, this);

	return this;
}
p = WhereNode.prototype;

p.set_is = function (key, value) {
	this.fields.push({
		op    : "=",
		key   : key,
		value : value
	});

	return this;
};

p.set_function = function (key, object) {
	var op = "=",
		final_value;

	Object.keys(object).forEach(function (key) {
		var value = object[key],
			fn;

		key = key.toLowerCase();

		if (key[0] === "$") {
			if (FUNCS[key] === void 0) {
				throw new Error("WhereNode - invalid function: " + key);
			}

			if (key === "$distinct") {
				delete object.op;
				op = "IN";
			}

			fn = FUNCS[key];
			final_value = fn(value, this.table);
		} else if (key === "op") {
			if (OPERATORS[value] === void 0) {
				throw new Error("WhereNode - invalid operator: " + value);
			}
			
			op = OPERATORS[value];
		}
	}, this);

	if (final_value === void 0) {
		throw new Error("WhereNode - Aggregate function not found.");
	}
	
	this.fields.push({
		op      : op,
		key     : key,
		value   : final_value,
		is_func : true
	});
};

p.set_ops = function (field, object) {
	Object.keys(object).forEach(function (key) {
		var value = object[key],
			op  = "=",
			fn;

		key = key.toLowerCase();

		if (key === "$fn") {
			this.set_function(field, value)
		} else if (FUNCS[key]) {
			fn = FUNCS[key];

			if (key === "$distinct") {
				op = "IN";
			}

			this.fields.push({
				op      : op,
				key     : field,
				value   : fn(value, this.table),
				is_func : true
			});
		} else if (OPERATORS[key]) {
			this.fields.push({
				op    : OPERATORS[key],
				key   : field,
				value : value
			});
		} else {
			throw new Error("WhereNode - invalid Aggregate function or operator: " + key);
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
