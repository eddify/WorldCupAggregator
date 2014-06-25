// 
var util = require('util'),
    twitter = require('twitter'),
    MongoClient = require('mongodb'),
    events = require('events');

//Create EventEmitter that batches incoming tweets and stores them into mongo, and reconnects on disconnection, errors, or stalls
var TwitterToMongo = function(options) {
    events.EventEmitter.call(this);

    var self = this;

    self.options = {};

    //specifies mongo URI which includes host, basic auth, and db name
    self.options.mongoUri = options.mongoUri || '';
    //specifies consumer key, secret, access tokey key and secret
    self.options.twitterAuth = options.twitterAuth || {};
    //specifies threshhold of tweets needed before committing to mongo db
    self.options.batchSize = options.batchSize || 500; 
    //specifies time interval to check if twitter stream is stalling in delivery
    self.options.stallCheckInterval = options.stallCheckInterval || 30000;
    //specifies amount of time to wait before reconnecting
    self.options.coolOffInterval = options.coolOffInterval || 5000; 
    //specifies if stream should be restarted if a stream error emitted
    self.options.reconnectOnError = options.reconnectOnError || true;
    //specifies if stream should be restarted if steam emits end event
    self.options.reconnectOnEnd = options.reconnectOnEnd || true;

    self.options.reconnectOnStall = options.reconnectOnStall || true;

    //specifies keywords to track
    self.options.keywords = options.keywords || '';

    self.twitter = new twitter(self.options.twitterAuth);
    self.tweetCounter = 0;
    self.mongoClient = MongoClient;
    self.state = 'ready';
    self.tweetBuffer = [];

    //checks for tweet count every x ms to see if tweet stream stalls ( common issue with api )

    self.stallCheckTimer = setInterval(function() {
        self.stallCheck( self.tweetCounter );
    }, self.options.stallCheckInterval)

    return self;
}

util.inherits(TwitterToMongo, events.EventEmitter);


TwitterToMongo.prototype.begin = function() {
    var self = this;

    self.mongoClient.connect(self.options.mongoUri, function(error, db) {
        if(error) return self.emit('error', error);

        self.tweetsCollection = db.collection('tweets');
        self.db = db;

        self.emit('db_connected')

        self.twitter.stream('user', {track: self.options.keywords} , function(stream) {
            self.emit('connected')

            stream.on('data', function(data) {
                
                //filter out non-tweets which do not have the created_at property
                if(!data.created_at) return;

                self.addTweet(data);
            });
            stream.on('error', function(error) {
                self.emit('error', error)
                
                if(this.options.reconnectOnError)
                    self.reconnect();
            })
            stream.on('end', function(reason) {
                self.emit('end')
                
                if(self.options.reconnectOnEnd)
                    self.reconnect()
            })
            self.activeTweetStream = stream;

        })
    })
}

TwitterToMongo.prototype.addTweet = function(tweet) {
    var self = this;
    self.tweetBuffer.push(tweet);

    self.tweetCounter += 1;
    self.emit('tweet_add', tweet)

    if(self.tweetBuffer.length >= self.options.batchSize)
    {
        //create local copy
        var tempBuffer = self.tweetBuffer.slice();
        self.tweetBuffer = [];

        self.tweetsCollection.insert(tempBuffer, { safe: true }, function(error, results) {
            if(error) {
                self.emit('error', error);

                if(this.options.reconnectOnError)
                    self.reconnect();

                return;
            }

            self.emit('batch_add', results)
        })

    }
}

TwitterToMongo.prototype.reconnect = function() {
    var self = this;

    //cleanup
    self.end()

    setTimeout(function() {
        self.emit('reconnect');
        self.begin();

    }, this.options.coolOffInterval)

}

TwitterToMongo.prototype.stallCheck = function(tweetCount) {
    var self = this;

    this.emit('stall_check', this.tweetCounter)
    if(tweetCount === 0)
    {
        self.emit('error', 'Tweet stream is stalling.')
        if(self.options.reconnectOnStall)
        {
            self.reconnect();
        }
    }
    else
        this.tweetCounter = 0;
}

TwitterToMongo.prototype.end = function() {
    var self = this;

    if(self.activeTweetStream)
    {
        self.activeTweetStream.destroy();
    }

    if(self.db)
    {
        self.db.close()
    }

    clearInterval(self.stallCheckTimer);
}


module.exports = TwitterToMongo;

