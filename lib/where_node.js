/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : where_node.js
* Purpose    :
* Created at : 2015-02-11
* Updated at : 2015-03-20
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
	WhereNode, p;

module.exports = WhereNode = function (node) {
	this.in      = {};
	this.fields  = [];
	this.to_sort = node.$sort || false;

	delete node.$sort;

	Object.keys(node).forEach(function (key) {
		var value = node[key];

		if (typeof(value) === "object") {
			if (value instanceof Array) {
				this.in[key] = value;
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

p.set_ops = function (key, value) {
	Object.keys(value).forEach(function (op) {
		op = op.toLowerCase();

		if (OPERATORS[op] === void 0) {
			throw new Error("WhereNode - invalid operator: " + op);
		}

		var val = value[op];

		this.fields.push({
			op    : OPERATORS[op],
			key   : key,
			value : val
		});
	}, this);
	
	return this;
};

p.build = function () {
	var query         = { str : "", values : [] },
		place_holders = [];
	
	this.fields.forEach(function (o) {
		place_holders.push("`" + o.key + "` " + o.op + " ?");
		query.values.push(o.value);
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
