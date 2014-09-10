#!/usr/bin/env node
// Copyright 2014 Nate Fitch, All rights reserved.

var big = require('big.js');
var Heap = require('heap');
var fs = require('fs');
var lstream = require('lstream');
var optimist = require('optimist');

var usage = [
    'Reduces a csv with many lines and numbers to fewer lines and numbers.',
    'Also does some helpful things like computing totals and dividing all',
    'numbers by some fixed amount (see the options below for details).'
].join('\n');

var ARGV = optimist.usage(usage).options({
    'd': {
        alias: 'divide',
        describe: 'divide all resulting numbers by this',
    },
    'i': {
        alias: 'input',
        describe: 'input csv',
        demand: true
    },
    'l': {
        alias: 'lines',
        describe: 'The number of lines you\'ll want, including the total.',
        'default': 25
    },
    'T': {
        alias: 'no_total',
        describe: 'don\'t include a total',
        'default': false
    }
}).argv;

var ls = fs.createReadStream(ARGV.i).pipe(new lstream());
var heap = new Heap(function (a, b) {
    return(a.last.minus(b.last));
});
var header = null;
var maxLines = ARGV.T ? ARGV.l : ARGV.l - 1;
var totals = [];
var divisor = ARGV.d ? big(ARGV.d) : null;

function handleLine(l) {
    var parts = l.split(',');
    if (header === null) {
        header = l;
        for (var i = 1; i < parts.length; ++i) {
            totals[i] = big(0);
        }
        return;
    }

    var o = {};
    o.line = l;
    o.last = big(parts[parts.length - 1]);
    o.name = parts[0];

    //Add parts to total...
    for (var i = 1; i < parts.length; ++i) {
        totals[i] = totals[i].plus(big(parts[i]));
    }


    if (heap.size() >= maxLines) {
        if (heap.peek().last.lt(o.last)) {
            heap.pop();
            heap.push(o);
        }
    } else {
        heap.push(o);
    }
}

function finishLine(l) {
    if (!divisor) {
        return (l);
    }
    var parts = l.split(',');
    var line = parts[0];
    for (var i = 1; i < parts.length; ++i) {
        line += ',' + big(parts[i]).div(divisor).toFixed(0);
    }
    return (line);
}

ls.on('readable', function () {
    var l;
    while (null !== (l = ls.read())) {
        handleLine(l);
    }
});

ls.on('end', function () {
    console.log(header);
    if (!ARGV.T) {
        // Put totals in here...
        var line = 'TOTALS';
        for (var i = 1; i < totals.length; ++i) {
            line += ',' + totals[i].toString();
        }
        // Yes, a little inefficient.  Meh.
        console.log(finishLine(line));
    }
    heap.toArray().forEach(function (o) {
        console.log(finishLine(o.line));
    });
});
