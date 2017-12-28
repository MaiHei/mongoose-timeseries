var mongoose = require('mongoose');
var Q = require('q');
mongoose.Promise = Q.Promise;

module.exports = {
    open: function() {
        var options = {
            useMongoClient: true
        };
        return mongoose.connect("mongodb://127.0.0.1/mongoose-timeseries-test", options).then(function () {
            return mongoose;
        });
    },

    close: function(done)
    {
        mongoose.connection.close(function() {
            done();
        });
    }
}