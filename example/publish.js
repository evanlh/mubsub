var mubsub = require('../lib/index');

var client = mubsub(process.env.MONGODB_URI || 'mongodb://localhost:27017/mubsub_example');
var channel = client.channel('example');

client.on('error', console.error);
channel.on('error', console.error);

setInterval(function () {
    console.log("trying!");
    channel.publish('foo', { foo: 'bar', time: Date.now() }, function (err) {
        if (err) {
          console.log(err.stack || err.toString());
          throw err;
        }
    });
}, 2000);
