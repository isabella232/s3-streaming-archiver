var knox = require("knox");
var mpu = require("knox-mpu");
var s3lister = require("s3-lister");
var async = require("async");
var archiver = require("archiver");
var moment = require("moment");

var sourceBucketList = process.env.S3_SOURCE_BUCKET.split(",");
var targetClient = knox.createClient({
  key: process.env.S3_ACCESS_KEY_ID,
  secret: process.env.S3_SECRET_KEY,
  bucket: process.env.S3_TARGET_BUCKET
});

async.eachSeries(sourceBucketList, function(bucket, bucketListCallback) {

  var archiveFileName = bucket + "-archive.tar",
      archiveNeedsUpdate = false,
      archiveFileUpdatedAt;

  targetClient.headFile(archiveFileName, function(err, res) {
    if (err) {
      console.log(err);
      archiveNeedsUpdate = true;
    } else {
      console.log(archiveFileName, "was last created on", res.headers['last-modified']);
      archiveFileUpdatedAt = Date.parse(res.headers['last-modified']);
    }
  });

  var sourceClient = knox.createClient({
    key: process.env.S3_ACCESS_KEY_ID,
    secret: process.env.S3_SECRET_KEY,
    bucket: bucket
  });

  var lister = new s3lister(sourceClient);
  var keys = [];

  lister.on('data', function(data) {
    if (!archiveNeedsUpdate) {
      if (archiveFileUpdatedAt && moment(Date.parse(data.LastModified)).isAfter(archiveFileUpdatedAt)) {
        archiveNeedsUpdate = true;
      }
    }
    keys.push(data.Key);
  });


  lister.on('end', function() {

    if (archiveNeedsUpdate) {

      console.log('==> Found newer file(s)! looping over '+keys.length+' keys...');

      var tarStream = archiver('tar');
      var upload = new mpu({
        client: targetClient,
        objectName: archiveFileName,
        stream: tarStream,
        maxRetries: 4
      }, function(err, body) {
        if (err) {
          console.log("Failed to complete upload", err);
        } else {
          console.log("Completed Upload", body);
        }
        bucketListCallback();
      });
      upload.on('uploading', function(number) { console.log("\n", "--> Starting part", number); });
      upload.on('error', console.log);
      // upload.on('uploaded', console.log);
      // upload.on('initiated', console.log);
      // upload.on('completed', function(a) {
      //   console.log("Completed Upload ", a);
      // });

      // for testing:
      // keys = keys.slice(1, 1000);

      var downloadQueue = async.queue(function(key, callback) {
        process.stdout.write(".");
        sourceClient.getFile(key, function(err, res) {
          if (err) {
            console.log("client.getFile Error on", key, err.code);
            downloadQueue.unshift(key); // try again
            callback(err);
          } else {
            tarStream.append(res, { name: key });
            res.on("end", callback);
          }
        }).on('error', function(err) {
          console.log("client.getFile Error Event on", key, err.code); 
          downloadQueue.unshift(key); // try again
          // callback(); // already got called on 'end'. so weird.
        });
      }, 10);

      downloadQueue.push(keys);

      var finalizeTarStream = function() {
        if (downloadQueue.idle()) {
          console.log("Finalizing tarStream");
          tarStream.finalize();
        }
      };

      downloadQueue.drain = finalizeTarStream;

    } else {
      console.log("==> Looks like all", keys.length, "files are older than the archive. Skipping.");
      bucketListCallback();
    }
  });

  lister.on('error', function(err) {
    console.log('==> Error!', err);
  });

});
