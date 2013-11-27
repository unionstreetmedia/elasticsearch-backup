'use strict';

//Standard Library
var zlib = require('zlib'),
    fs = require('fs');

//External
var prom = require('promiscuous-tool'),
    _ = require('lodash'),
    fstream = require('fstream'),
    tar = require('tar');

module.exports = {
    promiseWriteToFileStream: promiseWriteToFileStream,
    promiseEndFile: promiseEndFile,
    compress: compress,
    extract: extract,
    rmdirR: rmdirR,
    errorHandler: errorHandler,
    log: log
};

//Write to file stream and return promise
function promiseWriteToFileStream (fileStream, data) {
    return prom((fulfill, reject) => {
        process.stdout.write('\rwriting to ' + fileStream.path + ' : ' + fileStream.bytesWritten + ' bytes');
        if (fileStream.write(data)) {
            fulfill(fileStream);
        } else {
            fileStream.once('drain', () => fulfill(fileStream));
        }
    });
}

//End file stream and return promise
function promiseEndFile (fileStream) {
    return prom((fulfill, reject) => {
        fileStream.once('finish', fulfill);
        fileStream.end();
    });
}

function compress (filePath) {
    return prom((fulfill, reject) => {
        //tar and gzip the directory
        if (fs.existsSync(filePath)) {
            fstream.Reader({path: filePath, type: 'Directory'})
                .pipe(tar.Pack())
                .pipe(zlib.Gzip())
                .pipe(fstream.Writer(filePath + '.tar.gz'))
                .on('error', reject)
                .on('close', () => {
                    process.stdout.write('\ncompressed to ' + filePath + '.tar.gz \n');
                    fulfill(filePath);
                });
        } else {
            process.stdout.write('\nNo file to compressed to ' + filePath + '.tar.gz \n');
            fulfill(filePath);
        }
    });
}

function extract (file) {
    return prom((fulfill, reject) => {
        var filePath = file.substring(0, file.lastIndexOf('/'));
        //tar and gzip the directory
        if (fs.existsSync(file)) {
            fstream.Reader({path: file, type: 'file'})
                .pipe(zlib.Gunzip())
                .pipe(tar.Extract({path: filePath}))
                .on('error', reject)
                .on('close', () => {
                    process.stdout.write('\nextracted to ' + filePath + '\n');
                    fulfill(filePath);
                });
        } else {
            process.stdout.write('\nNo file to extract\n');
            fulfill(filePath);
        }
    });
}

//Recursively delete a directory
function rmdirR (path) {
    if (fs.existsSync(path)) {
        fs.readdirSync(path).forEach(function(file){
            var curPath = path + "/" + file;
            if (fs.lstatSync(curPath).isDirectory()) { // recurse
                rmdirR(curPath);
            } else { // delete file
                fs.unlinkSync(curPath);
            }
        });
        fs.rmdirSync(path);
    }
}

function errorHandler (error) {
    if (error.errno === 34 && error.code === 'ENOENT') {
        console.log('Target directory does not exist');
    } else {
        console.log(error);
    }
    return error
}

function log (foo) {
    console.log(foo);
    return foo;
}
