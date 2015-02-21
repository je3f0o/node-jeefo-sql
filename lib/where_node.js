/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : where_node.js
* Purpose    :
* Created at : 2015-02-11
* Updated at : 2015-02-22
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

module.exports = WhereNode = function (node, group_concat) {
	this.in        = {};
	this.fields    = [];
	this.groups    = [];

	this.group_concat = group_concat || "AND";

	if (node.$groups) {
		node.$groups.forEach(function (group) {
			this.groups.push(new WhereNode(group));
		}, this);

		delete node.$groups;
	}

	if (node.$or_groups) {
		node.$or_groups.forEach(function (group) {
			this.groups.push(new WhereNode(group, "OR"));
		}, this);

		delete node.$or_groups;
	}

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

p.build = function (order_fields) {
	var query         = { str : null, values : [], concat : this.group_concat },
		results       = [],
		place_holders = [];
	
	if (this.group_concat !== "AND" && this.group_concat !== "OR") {
		throw new Error("WhereNode.build group_concat value must be ('AND' or 'OR')");
	}

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

		if (order_fields) {
			order_fields.push({
				key          : key,
				values       : this.in[key],
				place_holder : temp_place_holders
			});
		}
	}, this);

	if (place_holders.length) {
		results.push({
			str : place_holders.join(" AND ")
		});
	}

	this.groups.forEach(function (group) {
		var q = group.build(order_fields);

		results.push({
			str    : q.str,
			concat : q.concat
		});
		query.values = query.values.concat(q.values);
	});

	query.str = null;
	results.forEach(function (r) {
		if (query.str) {
			query.str += " " + r.concat + " " + r.str;
		} else {
			query.str = "(" + r.str + ")";
		}
	}, this);

	return query;
};

if (require.main === module) {
	var main = new WhereNode({
		firstname : "John",
		lastname : "Doe",
		email : {
			$not : "person@example.com"
		},
		age : {
			$lte : 15,
			$gte : 20
		},
		id : [99,98,97],
		$groups : [
			{
				date : {
					$not : "something"
				},
				love : "Sugi",
				person_to_meet : {
					$like : "%jeefo%"
				},
				age : {
					$lt : 10,
					$gt : 20
				}
			}
		],
		$or_groups : [
			{
				person_to_love : "Sugi"
			}
		]
	});

	// console.log(child.build("AND"));
	console.log(main.build());
}
