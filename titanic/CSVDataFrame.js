var util = require('util');
var thenify = require('thenify');
var Table = require('cli-table');
var plot = require('plotter').plot;

function DataFrame(dataset, fields) {
	this.dataset = dataset;
	this.fields = fields;
	this.schema = {};
}

DataFrame.prototype.show = thenify(function(n, done) {
	var table = new Table({head: this.fields, colWidths: this.fields.map(n => 12)});
	this.dataset.take(n).then(function(result) {
		result.map(d => table.push(d));
		console.log(table.toString());
		done(null);
	});
});

DataFrame.prototype.select = function(fields) {
	if (!Array.isArray(fields)) throw new Error('DataFrame.select(): fields argument must be an instance of Array.');
	var fields_idx = [];
	for (var i in fields) {
		var idx = this.fields.indexOf(fields[i]);
		if (idx == -1) throw new Error('DataFrame.select(): field ' + fields[i] + ' does not exist.');
		fields_idx.push(idx);
	}

	function mapper(data, fields_idx) {
		var tmp = [];
		for (var i in fields_idx) tmp.push(data[fields_idx[i]]);
		return tmp;
	}

	return new DataFrame(this.dataset.map(mapper, fields_idx), fields);
}

DataFrame.prototype.where = function(expr) {
	// expr = {field: {$eq: value}}
	var fieldIdx = {};
	this.fields.map((f, i) => fieldIdx[f] = i);

	function filter(data, args) {
		var expr = args.expr, fieldIdx = args.fieldIdx;
		for (var field in expr) {
			if ((expr[field].$eq != undefined) && (data[args.fieldIdx[field]] != expr[field].$eq)) return false;
			if ((expr[field].$neq != undefined) && (data[args.fieldIdx[field]] == expr[field].$neq)) return false;
		}
		return true;
	}
	return new DataFrame(this.dataset.filter(filter, {fieldIdx: fieldIdx, expr: expr}), this.fields)
}

DataFrame.prototype.showSchema = thenify(function(done) {
	var self = this, schema = {};
	this.fields.map((f, i) => schema[f] = {idx: i, isReal: true, categories: []});

	function reducer(schema, data) {
		for (var field in schema) {
			var idx = schema[field].idx, value = data[idx];
			if (isNaN(Number(value))) {
				schema[field].isReal = false;
				// if (schema[field].categories.indexOf(value) == -1)
				// 	schema[field].categories.push(value);
			}
		}
		return schema;
	}

	function combiner(schema1, schema2) {
		for (var field in schema1) {
			if (schema1[field].isReal == undefined) schema1[field].isReal = schema2[field].isReal
			else schema1[field].isReal = schema1[field].isReal && schema2[field].isReal;
			// for (var i in schema2[field].categories)
			// 	if (schema1[field].categories.indexOf(schema2[field].categories[i]) == -1)
			// 		schema1[field].categories.push(schema2[field].categories[i])
		}
		return schema1;
	}

	this.dataset.aggregate(reducer, combiner, schema, function(err, schema) {
		for (var col in schema)
			console.log(schema[col].idx + ':' +  col + ' ' + (schema[col].isReal ? 'real-valued' : 'categorical'));
		done(err);
	});
});

// Ploting distribution as feature_name.png
// TODO, il faut substituer gnuplot à d3.js pour pauffiner l'affichage des courbes
// il faut renvoyer un tableau avec colonne = min, max, mean, stddev, type(real, categorical)
DataFrame.prototype.describe = thenify(function(field, done) {
	var idx = this.fields.indexOf(field);
	this.dataset.map((data, idx) => data[idx], idx)
		.map(function(feature) {return isNaN(Number(feature)) ? feature : Number(feature);})
		.countByValue().then(function(tmp) {
			if (isNaN(Number(tmp[0][0]))) {					// Discrete feature
				tmp.sort(function(a, b) {return b[1] - a[1]});	// Sort descent
				var xy = [];
				for (var i in tmp) xy[i] = tmp[i][1];
			} else {											// Continuous feature
				tmp.sort();
				var xy = {};
				for (var i in tmp) xy[tmp[i][0]] = tmp[i][1];
			}

			var data = {'': xy};

			plot({
				title: field + ' distribution',
				data: data,
				style: 'boxes',
				filename: field + '.png',
				finish: function() {
					console.log('Creating ' + field + '.png');
					done(null);
				}
			});
		});
});

DataFrame.prototype.number_encode_features = function() {
	function reducer(schema, data) {
		for (var field in schema) {
			var idx = schema[field].idx, value = data[idx];
			if (isNaN(Number(value))) {
				schema[field].isReal = false;
				if (schema[field].categories.indexOf(value) == -1)
					schema[field].categories.push(value);
			}
		}
		return schema;
	}

	function combiner(schema1, schema2) {
		for (var field in schema1) {
			if (schema1[field].isReal == undefined) schema1[field].isReal = schema2[field].isReal
			else schema1[field].isReal = schema1[field].isReal && schema2[field].isReal;
			for (var i in schema2[field].categories)
				if (schema1[field].categories.indexOf(schema2[field].categories[i]) == -1)
					schema1[field].categories.push(schema2[field].categories[i])
		}
		return schema1;
	}

	var schema = {};
	this.fields.map((f, i) => schema[f] = {idx: i, isReal: true, categories: []});
	var dataset = this.dataset
		.map(a => [1, a])
		.aggregateByKey(reducer, combiner, schema)
		.map(a => a[1])
		.cartesian(this.dataset)
		.map(function(data) {
			var schema = data[0], features = data[1];
			var tmp = [];
			for (var field in schema) {
				var value = features[schema[field].idx];
				tmp.push(schema[field].isReal ? Number(value) : schema[field].categories.indexOf(value));
			}
			return tmp;			
		});
	
	return new DataFrame(dataset, this.fields);
};

DataFrame.prototype.drop = function(fields) {
	for (var i in fields)
		if (this.fields.indexOf(fields[i]) == -1)
			throw new Error('DataFrame.drop(): field ' + fields[i] + ' does not exist.')

	var newFields = [], newFields_idx = [];
	for (var i in this.fields)
		if (fields.indexOf(this.fields[i]) == -1) {
			newFields_idx.push(i);
			newFields.push(this.fields[i]);
		}

	return new DataFrame(this.dataset
		.map(function(data, newFields_idx) {
			var tmp = [];
			for (var i in newFields_idx) tmp.push(data[newFields_idx[i]]);
			return tmp;
	}, newFields_idx), newFields);
}

DataFrame.prototype.toLabeledPoint = function(label, features) {
	// Check if features is an array
	if (!Array.isArray(features))
		throw new Error('DataFrame.toLabeledPoint(): features argument must be an instance of Array.');
	// Check if label et features exists in data frame fields
	if (this.fields.indexOf(label) == -1) 
		throw new Error('toLabeledPoint(): field ' + label + ' does not exist.')
	for (var i in features)
		if ((features[i] != '*') && this.fields.indexOf(features[i]) == -1) 
			throw new Error('toLabeledPoint(): field ' + features[i] + ' does not exist.')
	// check if label is not in features
	if (features.indexOf(label) != -1)
		throw new Error('toLabeledPoint(): features must not include label.')
	// if * is used as features, build a vector containing all fields except label
	var tmp = [];
	if ((features.length == 1) && (features[0] == "*")) {
		for (var i in this.fields)
			if (this.fields[i] != label) tmp.push(this.fields[i]);
		features = tmp;
	}

	return this.dataset.map(function(data, args) {
		var features = [];
		for (var i in args.features)
			features.push(Number(data[args.fields.indexOf(args.features[i])]));
		return [data[args.fields.indexOf(args.label)] * 2 - 1, features]	// ICI on force à -1/1
	}, {fields: this.fields, label: label, features: features})
}

DataFrame.prototype.take = thenify(function(n, done) {
	this.dataset.take(n, done);
});

// Old version of CSV dataframe
// DataFrame.call(this, sc.textFile(file)
// 	.map((line, sep) => line.split(sep).map(str => str.trim()), sep)			// split csv lines on separator
// 	.filter((data, na_values) => data.indexOf(na_values) == -1, na_values),		// ignore lines containing na_values
// fields);

/*
	path: location of files,
	header: when set to true the first line of files will be used to name columns and will not be included in data. 
			All types will be assumed string. Default value is false.	
	delimiter: by default columns are delimited using ,, but delimiter can be set to any character
	quote: by default the quote character is ", but can be set to any character. Delimiters inside quotes are ignored	
	escape: by default the escape character is \, but can be set to any character. Escaped quote characters are ignored
	mode: determines the parsing mode. By default it is PERMISSIVE. Possible values are:
		PERMISSIVE: tries to parse all lines: nulls are inserted for missing tokens and extra tokens are ignored.
		DROPMALFORMED: drops lines which have fewer or more tokens than expected or tokens which do not match the schema
		FAILFAST: aborts with a RuntimeException if encounters any malformed line	
*/
function CSVDataFrame2(sc, path, fields, options) {
	var header = options.header || false;
	var sep = options.sep || ',';
	var quote = options.quote || '"';
	var escape = options.escape || '\\';
	var mode = options.mode || 'PERMISSIVE';

	DataFrame.call(this, sc.textFile(path).map(CSVtoArray), fields);

	function CSVtoArray (csvString) {
	    var fieldEndMarker  = /([,\015\012] *)/g; 				/* Comma is assumed as field separator */
	    var qFieldEndMarker = /("")*"([,\015\012] *)/g; 		/* Double quotes are assumed as the quote character */
	    var startIndex = 0;
	    var records = [], currentRecord = [];
	    do {
	        // If the to-be-matched substring starts with a double-quote, use the qFieldMarker regex, otherwise use fieldMarker.
	        var endMarkerRE = (csvString.charAt (startIndex) == '"')  ? qFieldEndMarker : fieldEndMarker;
	        endMarkerRE.lastIndex = startIndex;
	        var matchArray = endMarkerRE.exec (csvString);
	        if (!matchArray || !matchArray.length) {
	            break;
	        }
	        var endIndex = endMarkerRE.lastIndex - matchArray[matchArray.length-1].length;
	        var match = csvString.substring (startIndex, endIndex);
	        if (match.charAt(0) == '"') { // The matching field starts with a quoting character, so remove the quotes
	            match = match.substring (1, match.length-1).replace (/""/g, '"');
	        }
	        currentRecord.push (match);
	        var marker = matchArray[0];
	        if (marker.indexOf (',') < 0) { // Field ends with newline, not comma
	            records.push (currentRecord);
	            currentRecord = [];
	        }
	        startIndex = endMarkerRE.lastIndex;
	    } while (true);
	    if (startIndex < csvString.length) { // Maybe something left over?
	        var remaining = csvString.substring (startIndex).trim();
	        if (remaining) currentRecord.push (remaining);
	    }
	    if (currentRecord.length > 0) { // Account for the last record
	        records.push (currentRecord);
	    }
	    return records[0];
	};
}

function CSVDataFrame(sc, fields, file, sep, na_values) {
	/*
	charset: defaults to 'UTF-8' but can be set to other valid charset names
	inferSchema: automatically infers column types. It requires one extra pass over the data and is false by default
	comment: skip lines beginning with this character. Default is "#". Disable comments by setting this to null.
	nullValue: specificy a string that indicates a null value, any fields matching this string will be set as nulls in the DataFrame
	dateFormat: specificy a string that indicates a date format. Custom date formats follow the formats at java.text.SimpleDateFormat. This applies to both DateType and TimestampType. By default, it is null which means trying to parse times and date by java.sql.Timestamp.valueOf() and java.sql.Date.valueOf().
	*/
	DataFrame.call(this, sc.textFile(file).map(CSVtoArray), fields);

	function CSVtoArray (csvString) {
	    var fieldEndMarker  = /([,\015\012] *)/g; 				/* Comma is assumed as field separator */
	    var qFieldEndMarker = /("")*"([,\015\012] *)/g; 		/* Double quotes are assumed as the quote character */
	    var startIndex = 0;
	    var records = [], currentRecord = [];
	    do {
	        // If the to-be-matched substring starts with a double-quote, use the qFieldMarker regex, otherwise use fieldMarker.
	        var endMarkerRE = (csvString.charAt (startIndex) == '"')  ? qFieldEndMarker : fieldEndMarker;
	        endMarkerRE.lastIndex = startIndex;
	        var matchArray = endMarkerRE.exec (csvString);
	        if (!matchArray || !matchArray.length) {
	            break;
	        }
	        var endIndex = endMarkerRE.lastIndex - matchArray[matchArray.length-1].length;
	        var match = csvString.substring (startIndex, endIndex);
	        if (match.charAt(0) == '"') { // The matching field starts with a quoting character, so remove the quotes
	            match = match.substring (1, match.length-1).replace (/""/g, '"');
	        }
	        currentRecord.push (match);
	        var marker = matchArray[0];
	        if (marker.indexOf (',') < 0) { // Field ends with newline, not comma
	            records.push (currentRecord);
	            currentRecord = [];
	        }
	        startIndex = endMarkerRE.lastIndex;
	    } while (true);
	    if (startIndex < csvString.length) { // Maybe something left over?
	        var remaining = csvString.substring (startIndex).trim();
	        if (remaining) currentRecord.push (remaining);
	    }
	    if (currentRecord.length > 0) { // Account for the last record
	        records.push (currentRecord);
	    }
	    return records[0];
	};

	// this.select2 = thenify(function(name, number, done) {
	// 	var idx = self.features.indexOf(name), tmp = [];
	// 	self.data.count().on('data', function(count) {
	// 		self.data
	// 			.map((data, args) => data[args.idx], {idx: idx})
	// 			.map(function(feature) {return isNaN(Number(feature)) ? feature : Number(feature);})
	// 			.countByValue()
	// 			.on('data', function(data) {
	// 				tmp.push([data[0], Math.round(data[1] / count * 1000000 ) / 1000000]);
	// 			})
	// 			.on('end', function() {
	// 				tmp.sort(function(a, b) {return b[1] - a[1]});	// Sort descent
	// 				tmp = tmp.slice(0, number);
	// 				var table = new Table({head: [name, 'Percentage'], colWidths: [20, 20]});
	// 				tmp.map(d => table.push(d));
	// 				console.log(table.toString());
	// 				done(null);
	// 			});
	// 		});
	// });
}

util.inherits(CSVDataFrame, DataFrame);

module.exports = CSVDataFrame;