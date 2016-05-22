'use strict';

const _stash = {};

const makeStashKey = (items) => items.join('%%%%');
const getItemFromKey = (key) => key.split("%%%%");

const Stash = function Stash(count) {
    this.bucket = count;
    this._stash = {};
};

Stash.prototype.push = function _push(keyItems, item) {
    const key = makeStashKey(keyItems);
    if (!this._stash[key]) this._stash[key] = [];

    this._stash[key].push(item);
};

Stash.prototype.flush = function _flush(size) {
    if (size === undefined) size = this.bucket;

    return Object
        .keys(this._stash)
        .filter(key => this._stash[key].length >= size)
        .map(key => {
            var o = { keys: getItemFromKey(key), items: this._stash[key] };
            this._stash[key] = [];
            return o;
        })
};


Stash.prototype.flushRemainder = function _flush() {
    return this.flush(0);
};


module.exports = Stash;
