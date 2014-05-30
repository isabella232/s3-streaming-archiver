var knox = require("knox");
var mpu = require("knox-mpu");
var s3lister = require("s3-lister");
var async = require("async");
var archiver = require("archiver");

var sourceBucketList = process.env.S3_SOURCE_BUCKET.split(",");
var targetClient = knox.createClient({
  key: process.env.S3_ACCESS_KEY_ID,
  secret: process.env.S3_SECRET_KEY,
  bucket: process.env.S3_TARGET_BUCKET
});

async.eachSeries(sourceBucketList, function(bucket, bucketListCallback) {

  var sourceClient = knox.createClient({
    key: process.env.S3_ACCESS_KEY_ID,
    secret: process.env.S3_SECRET_KEY,
    bucket: bucket
  });

  var lister = new s3lister(sourceClient);
  var keys = [];

  lister.on('data', function(data) {
    if (data.Key.indexOf(".tar") === -1) {
      keys.push(data.Key);
    }
  });

  lister.on('end', function() {
    console.log('==> Done!, looping over '+keys.length+' keys...');

    var tarStream = archiver('tar');
    var upload = new mpu({
      client: targetClient,
      objectName: bucket + '-archive.tar',
      stream: tarStream
    }, function(err, body) {
      console.log("Upload callback", err, body);
    });
    upload.on('uploading', console.log);
    upload.on('uploaded', console.log);
    upload.on('error', console.log);
    upload.on('initiated', console.log);
    upload.on('completed', function(a) {
      console.log("Completed Upload ", a);
      bucketListCallback();
    });

    // for testing:
    // keys = keys.slice(1, 1000);

    var tarStreamQueue = async.queue(function(fileObject, callback) {
      tarStream.append(fileObject.stream, { name: fileObject.name });
      callback();
    }, 1);

    var downloadQueue = async.queue(function(key, callback) {
      process.stdout.write(key + " .:. ");
      sourceClient.getFile(key, function(err, res) {
        if (err) {
          console.log("client.getFile Error on ", key, err);
          downloadQueue.push(key); // try again
          callback();
        } else {
          tarStreamQueue.push({ stream: res, name: key } );
          res.on("end", callback);
        }
      });
    }, 20);

    downloadQueue.push(keys);

    var finalizeTarStream = function() {
      if (tarStreamQueue.idle() && downloadQueue.idle()) {
        console.log("Finalizing tarStream");
        tarStream.finalize();
      }
    };

    tarStreamQueue.drain = finalizeTarStream;
    downloadQueue.drain = finalizeTarStream;
  });

  lister.on('error', function(err) {
    console.log('==> Error!', err);
  });

});
