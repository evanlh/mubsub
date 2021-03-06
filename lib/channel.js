var EventEmitter = require('events').EventEmitter;
var util = require('util');
var Promise = require('./promise');

var noop = function () {};

/**
 * Channel constructor.
 *
 * @param {Connection} connection
 * @param {String} [name] optional channel/collection name, default is 'mubsub'
 * @param {Object} [options] optional options
 *   - `size` max size of the collection in bytes, default is 5mb
 *   - `max` max amount of documents in the collection
 *   - `retryInterval` time in ms to wait if no docs found, default is 200ms
 *   - `recreate` recreate the tailable cursor on error, default is true
 * @api public
 */
function Channel(connection, name, options) {
    options || (options = {});
    options.capped = true;
    // In mongo v <= 2.2 index for _id is not done by default
    options.autoIndexId = true;
    options.size || (options.size = 1024 * 1024 * 5);
    options.retryInterval || (options.retryInterval = 200);
    options.recreate != null || (options.recreate = true);
    options.strict = false;

    this.options = options;
    this.connection = connection;
    this.closed = false;
    this.listening = null;
    this.name = name || 'mubsub';

    this.create().listen();
}

module.exports = Channel;
util.inherits(Channel, EventEmitter);

/**
 * Close the channel.
 *
 * @return {Channel} this
 * @api public
 */
Channel.prototype.close = function () {
    this.closed = true;

    return this;
};

/**
 * Publish an event.
 *
 * @param {String} event
 * @param {Object} [message]
 * @param {Function} [callback]
 * @return {Channel} this
 * @api public
 */
Channel.prototype.publish = function (event, message, callback) {
    var options = callback ? { safe: true } : {};
    callback || (callback = noop);

    this.ready(function (collection) {
        collection.insert({ event: event, message: message }, options, function (err, docs) {
            if (err) return callback(err);
            callback(null, docs[0]);
        });
    });

    return this;
};

/**
 * Subscribe an event.
 *
 * @param {String} [event] if no event passed - all events are subscribed.
 * @param {Function} callback
 * @return {Object} unsubscribe function
 * @api public
 */
Channel.prototype.subscribe = function (event, callback) {
    var self = this;

    if (typeof event == 'function') {
        callback = event;
        event = 'message';
    }

    this.on(event, callback);

    return {
        unsubscribe: function () {
            self.removeListener(event, callback);
        }
    };
};

/**
 * Create a channel collection.
 *
 * @return {Channel} this
 * @api private
 */
Channel.prototype.create = function () {
    var self = this;

    function create() {
        self.connection.db.createCollection(
            self.name,
            self.options,
            self.collection.resolve.bind(self.collection)
        );
    }

    this.collection = new Promise();
    this.connection.db ? create() : this.connection.once('connect', create);

    return this;
};

/**
 * Create a listener which will emit events for subscribers.
 * It will listen to any document with event property.
 *
 * @param {Object} [latest] latest document to start listening from
 * @return {Channel} this
 * @api private
 */
Channel.prototype.listen = function (latest) {
    var self = this;

    var connect = function(){
        var next = function (doc) {
            if (!doc) return;
            latest = doc;
            if (doc.event) {
                self.emit(doc.event, doc.message);
                self.emit('message', doc.message);
            }
            self.emit('document', doc);
        };

        var openCursor = function (latest, collection) {
            try {
                var cursor = collection.find(latest ? { _id: { $gt: latest._id }} : null, {
                    tailable: true,
                    numberOfRetries: -1,
                    awaitdata: true,
                    tailableRetryInterval: self.options.retryInterval

                }).maxTimeMS(20000).stream();
            }
            catch (e){
                self.emit('error', e);
                return;
            }
            cursor.on('data', next);
            cursor.on('close', function() {
                /* It seems (thru trial&error) that there is a cursor
                 * timeout of ~5 seconds, not yet sure how to override.
                 * It doesn't seem right to fire an error, so we
                 * attempt to reconnect using the latest document.
                 * If this succeeds the 'ready' event will refire. */
                // TODO test mem usage
                self.emit('close');
                connect();
            });
            cursor.on('error', function(err) {
                self.emit('error', err);
                if (self.options.recreate) {
                    self.create().listen(latest);
                }
            });

            self.listening = collection;
            self.emit('ready', collection);
        };
        self.latest(latest, self.handle(true, openCursor));
    };

    connect();

    return this;
};

/**
 * Get the latest document from the collection.
 *
 * @param {Object} [latest] latest known document
 * @param {Function} callback
 * @return {Channel} this
 * @api private
 */
Channel.prototype.latest = function (latest, callback) {
    var self = this;

    this.collection.then(function (err, collection) {
        if (err) return callback(err);

        var cursor = collection
            .find(latest ? { _id: latest._id } : null)
            .sort({ $natural: -1 })
            .limit(1);
        var next = cursor.next(function (err, doc) {
                cursor.close();
                callback(err, doc, collection);
            });
        if (next === null) callback(null, null, collection);
    });

    return this;
};

/**
 * Return a function which will handle errors and consider channel and connection
 * state.
 *
 * @param {Boolean} [exit] if error happens and exit is true, callback will not be called
 * @param {Function} callback
 * @return {Function}
 * @api private
 */
Channel.prototype.handle = function (exit, callback) {
    var self = this;

    if (typeof exit === 'function') {
        callback = exit;
        exit = null;
    }

    return function () {
        if (self.closed || self.connection.destroyed) return;

        var args = [].slice.call(arguments);
        var err = args.shift();

        if (err) self.emit('error', err);
        if (err && exit) return;

        callback.apply(self, args);
    };
};

/**
 * Call back if collection is ready for publishing.
 *
 * @param {Function} callback
 * @return {Channel} this
 * @api private
 */
Channel.prototype.ready = function (callback) {
    if (this.listening) {
        callback(this.listening);
    } else {
        this.once('ready', callback);
    }

    return this;
};
