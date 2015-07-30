/* -.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.-.
* File Name  : fields.js
* Purpose    :
* Created at : 2015-07-30
* Updated at : 2015-07-30
* Author     : jeefo
_._._._._._._._._._._._._._._._._._._._._.*/

"use strict";

module.exports = function (data) {
	return Object.keys(data).map(function (key) {
		return {
			key   : key,
			value : data[key]
		}
	});
};
