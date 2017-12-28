var assert = require('chai').assert;
var MTI = require('../');
var moment = require('moment');
var test_mongoose = require('./test-mongoose.js');
var mti;

//mongoose.connect('mongodb://localhost/mti');
//mongoose.connection.on('error', console.error.bind(console, 'connection error:'));

describe('hours push -', function () {

    before(function (done) {

        test_mongoose.open().then(function (mongoose) {
            mti = new MTI(mongoose,'test', {interval: 3600, postProcessImmediately: true});
            mti.model.remove({}, function () {
                done();
            });
        }).catch((err) => assert.fail())

    });
    
    //After all tests are finished drop database and close connection
    after(function(done){
        test_mongoose.close(done);
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

                mti.push(moment.utc(Date.UTC(2013, 6, 16, i)),
                    i,
                    {test: i},
                    false,
                    function (error, doc) {
                        //console.log('Hour: '+i);
                        //console.log(Object.keys(doc.hourly));
                        assert.typeOf(error, 'null');
                        assert.typeOf(doc, 'object');
                        assert.equal(doc.day.getTime(), new Date(2013, 6, 16).getTime());
                        assert.equal(doc.latest.value, i, 'Latest');
                        assert.equal(doc.hourly[i].value, i, 'current');

                        //assert.equal( Object.keys(doc.hourly).length, i+1);

                        //console.log('Loop '+i+' OK.');
                        loop(i + 1, count, cb);
                    });
            } else {
                cb();
            }
        }
        loop(0, 10, function () {
            done();
        });
    });

    it('doc count', function (done) {
        //collection
        mti.model.find({}, function (e, docs) {
            assert.typeOf(e, 'null');
            assert.typeOf(docs, 'array');
            assert.equal(docs.length, 1);
            //console.log('stats: '+JSON.stringify(docs[0].statistics));
            assert.equal(docs[0].statistics.count, 10);
            assert.equal(docs[0].statistics.min.value, 0);
            assert.equal(docs[0].statistics.max.value, 9);
            assert.equal(docs[0].statistics.sum/docs[0].statistics.count, 4.5); //avg to be done
            done();
        });
    });

    it('findMin', function (done) {
        //collection
        mti.findMin({
            from: moment.utc(Date.UTC(2013, 6, 16)),
            to: moment.utc(Date.UTC(2013, 6, 16))
        }, function (e, min) {
            assert.typeOf(e, 'null');
            assert.equal(min.value, 0);
            done();
        })
    });

    it('findMax', function (done) {
        //collection
        mti.findMax({
            from: moment.utc(Date.UTC(2013, 6, 16)),
            to: moment.utc(Date.UTC(2013, 6, 16))
        }, function (e, max) {
            assert.typeOf(e, 'null');
            assert.equal(max.value, 9);
            done();
        })
    });
});
