var mubsub = require('../lib/index');

var client = mubsub(process.env.MONGODB_URI || 'mongodb://localhost:27017/mubsub_example');
var channel = client.channel('example');
var handleError = function(err){
  console.error(err);
  console.error(err.stack);
};

channel.on('error', handleError);
client.on('error', handleError);

channel.subscribe('foo', function (message) {
    console.log(message);
});
