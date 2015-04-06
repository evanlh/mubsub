var mubsub = require('../lib/index');

var client = mubsub(process.env.MONGODB_URI || 'mongodb://localhost:27017/mubsub_example');
var channel = client.channel('example');
var handleError = function(err){
  console.error(err);
  console.error(err.stack);
};

channel.on('error', handleError);
channel.on('ready', function(){ console.log("Channel ready"); });
channel.on('close', function(){ console.log("Channel closed"); });
client.on('error', handleError);

var messages = 0;
channel.subscribe('foo', function (message) {
    //console.log(message);
    messages++;
});

setInterval(function(){ console.log(messages); messages = 0; }, 1000);
