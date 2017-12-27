var assert = require('chai').assert;
var mongoose = require('mongoose');
mongoose.Promise = require('q').Promise;
var MTI = require('../');
var moment = require('moment');
var mti;

//mongoose.connect('mongodb://localhost/mti');
//mongoose.connection.on('error', console.error.bind(console, 'connection error:'));

describe('daily -', function () {

    before(function (done) {
        var options = {
            useMongoClient: true
          };
        mongoose.connect("mongodb://127.0.0.1/mongoose-timeseries-test",options).then(function () {
            mti = new MTI(mongoose,'daily', {interval: 86400/* 24h */, postProcessImmediately: true});
            mti.model.remove({}, function () {
                done();
            });
        }).catch((err) => assert.fail())
    });

    it('init', function (done) {
        mti.model.count({}, function (e, c) {
            assert.typeOf(e, 'null');
            assert.equal(c, 0);
            done();
        });
    });

    it('pushes', function (done) {
        var loop = function (i, count, cb) {
            if (i < count) {

                mti.push(moment(new Date(2013, 6, i, 0)),
                    i,
                    {test: i},
                    false,
                    function (error, doc) {
                        //console.log('Hour: '+i);
                        //console.log(Object.keys(doc.hourly));
                        assert.typeOf(error, 'null');
                        assert.typeOf(doc, 'object');
                        assert.equal(doc.day.getTime(), new Date(2013, 6, i).getTime());
                        assert.equal(doc.latest.value, i, 'Latest');

                        //console.log('Loop '+i+' OK.');
                        loop(i + 1, count, cb);
                    });
            } else {
                cb();
            }
        }
        loop(1, 11, function () {
            done();
        });
    });

    it('doc count', function (done) {
        //collection
        mti.model.find({}, function (e, docs) {
            assert.typeOf(e, 'null');
            assert.typeOf(docs, 'array');
            assert.equal(docs.length, 10);
            //console.log('stats: '+JSON.stringify(docs[0].statistics));
            var i;
            for (i = 0; i < 10; i++) {
                assert.equal(docs[i].statistics.i, 1);
                assert.equal(docs[i].statistics.min.value, i + 1);
                assert.equal(docs[i].statistics.max.value, i + 1);
                assert.equal(docs[i].statistics.avg, i + 1);
            }
            done();
        });
    });

    it('findMin', function (done) {
        //collection
        mti.findMin({from: new Date(2013, 6, 0),
            to: new Date(2013, 6, 16)
        }, function (e, min) {
            assert.typeOf(e, 'null');
            assert.equal(min.value, 1);
            done();
        })
    });

    it('findMax', function (done) {
        //collection
        mti.findMax({from: new Date(2013, 6, 0),
            to: new Date(2013, 6, 16)
        }, function (e, max) {
            assert.typeOf(e, 'null');
            assert.equal(max.value, 10);
            done();
        })
    });
});
describe('daily fetch', function () {
    //var format = 'hash'
    //var format = '[x,y]'
    //var format = '[ms,y]'

    it('getData - format[x,y]', function (done) {
        mti.findData({from: moment(new Date(2013, 5, 30)),
            to: moment(new Date(2013, 6, 10)),
            condition: {},
            format: '[x,y]'
        }, function (e, data) {
            //console.log(data);
            assert.typeOf(e, 'null');
            assert.typeOf(data, 'array');
            assert.equal(data.length, 10);
            var i = 1;
            data.forEach(function (row) {
                assert.typeOf(row, 'array');
                assert.equal(row.length, 2);
                assert.equal(row[1], i++);
                assert.typeOf(row[0], 'Date');
                assert.typeOf(row[1], 'number');
            });
            done();
        });
    });

    it('getData (half period)-format[x,y]', function (done) {
        mti.findData({from: moment(new Date(2013, 6, 2)),
            to:moment( new Date(2013, 6, 4)),
            condition: {},
            format: '[x,y]'
        }, function (e, data) {
            assert.typeOf(e, 'null');
            assert.typeOf(data, 'array');
            assert.equal(data.length, 3);
            var i = 2;
            data.forEach(function (row) {
                assert.typeOf(row, 'array');
                assert.equal(row.length, 2);
                assert.equal(row[1], i++);
                assert.typeOf(row[0], 'Date');
                assert.typeOf(row[1], 'number');
            });
            done();
        });
    });

    it('getData-format[ms,y]', function (done) {
        mti.findData({from: moment(new Date(2013, 6, 1)),
            to: moment(new Date(2013, 6, 6)),
            condition: {},
            format: '[ms,y]'
        }, function (e, data) {
            //console.log(data);
            assert.typeOf(e, 'null');
            assert.typeOf(data, 'array');
            assert.equal(data.length, 6);
            var i = 1;
            data.forEach(function (row) {
                assert.typeOf(row, 'array');
                assert.equal(row.length, 2);
                assert.equal(row[1], i++);
                assert.typeOf(row[0], 'number');
            });
            done();
        });
    });
});
