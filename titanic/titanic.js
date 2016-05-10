#!/usr/bin/env node

var co = require('co');
var sc = require('skale-engine').context();
var plot = require('plotter').plot;

var CSVDataFrame = require('./CSVDataFrame.js');
var StandardScaler = require('skale-ml').StandardScaler;
var LogisticRegressionWithSGD = require('skale-ml').LogisticRegressionWithSGD;
var BinaryClassificationMetrics = require('skale-ml').BinaryClassificationMetrics;

co(function* () {
	var fields = ['PassengerId', 'Survived', 'Pclass', 'Name', 'Sex', 'Age',  'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', 'Embarked'];
	var df = new CSVDataFrame(sc, fields, 'train.csv', ',', '?');

	// drop some fields
	var df = df.drop(['PassengerId', 'Name', 'Ticket', 'Cabin', 'Embarked']);
	yield df.show(10);
	console.log(yield df.dataset.count());

	// ********************************************************************************************************* //
	// Remove incomplete lines
	// ********************************************************************************************************* //
	// yield df.where({"Pclass": {$eq: ''}}).show(10)		// crash

	df = df.where({"Age": {$neq: ''}})
	yield df.show(10)
	console.log(yield df.dataset.count())

	df = df.where({"Survived": {$neq: ''}})
	yield df.show(10)
	console.log(yield df.dataset.count())

	df = df.where({"Pclass": {$neq: ''}})
	yield df.show(10)
	console.log(yield df.dataset.count())
	// ********************************************************************************************************* //
	// console.log('\n# Encode the categorical features');	
	// var edf = df.number_encode_features();
	// yield edf.show(10)

	// console.log('# Extract a LabeledPoint Dataset from our encoded Data Frame');
	// var training_set = edf.toLabeledPoint("Survived", ["*"]);
	// // yield training_set.take(10).then(console.log)

	// // filter people with age 0
	// training_set = training_set.filter(data => data[1][2]);
	// // yield training_set.take(10).then(console.log)	

	// console.log('# Scale features to zero-mean, unit variance')
	// var scaler = new StandardScaler();
	// yield scaler.fit(training_set.map(p => p[1]));
	// var training_set_std = training_set.map((p, scaler) => [p[0], scaler.transform(p[1])], scaler).persist();

	// console.log('\n# Train logistic regression with SGD on standardized training set')
	// var nIterations = 10;
	// var parameters = {regParam: 0.01, stepSize: 1};
	// var model = new LogisticRegressionWithSGD(training_set_std, parameters);
	// yield model.train(nIterations);

	// var predictionAndLabels = training_set_std.map((p, model) => [model.predict(p[1]), p[0]], model);
	// var metrics = new BinaryClassificationMetrics(predictionAndLabels);

	// var roc = yield metrics.roc();
	// var xy = {};
	// for (var i in roc) xy[roc[i][1][0].toFixed(2)] = roc[i][1][1].toFixed(2);
	// xy['0.00'] = '0.00';
	// var data = {};
	// data['regParam: ' + parameters.regParam + ', stepSize: ' + parameters.stepSize] = xy;
	// data['Random'] = {0 :0, 1 : 1};
	// plot({title: 'Logistic Regression ROC Curve', data: data, filename: 'roc.png', finish: function() {;}});	

	// console.log(model.weights)	

}).then(function (value) {sc.end();}, function (err) {console.error(err.stack); sc.end();});