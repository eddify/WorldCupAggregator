var TwitterToMongo = require('./lib/twittertomongo'),
    util = require('util');

var tweetStream = new  TwitterToMongo({
    twitterAuth: {
        consumer_key: 'OzYo9yVsdPCjmdJioWh3Qw',
        consumer_secret: 'PkBLWSKtg8gli0qOIDAjJuvxeZ6M8SjzcYXMlPwxf4',
        access_token_key: '918354384-tYmdVh1EXzXjVerRJMizn01Bg19XozBbHbiOi69q',
        access_token_secret: 'IY9jOT1GIrCeChJbfv8htt0aoM4N5DCcljjlRIR76g'
    },
    mongoUri: 'mongodb://localhost:27017/worldcup',
    keywords: 'worldcup,worldcup2014'

})

tweetStream.on('error', function(error) {
    console.log('error: ' + util.inspect(error))
});

tweetStream.on('reconnect', function() {
    console.log('reconnecting...')
})
tweetStream.on('tweet_add', function(tweet) {
    console.log('adding tweet: ' + tweet.text)
})
tweetStream.on('batch_add', function(tweets) {
    console.log('' + tweets.length + ' tweets inserted to mongo!')
})

tweetStream.on('stall_check', function(tweetCounter) {
    console.log('stall check, tweets added in interval: ' + tweetCounter)
})

tweetStream.on('connected', function() {
    console.log('twitter stream connected!')
})

tweetStream.on('end', function() {
    console.log( 'twitter stream ending!')
})

tweetStream.on('db_connected', function() {
    console.log('mongo connected!')
})

tweetStream.begin();