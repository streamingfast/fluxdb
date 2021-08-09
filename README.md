# StreamingFast FluxDB
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/streamingfast/search)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A temporal database framework for blockchain state.

### Concepts

FluxDB aims at easily storing blockchain state at any block height, enabling
developers to retrieve current as well as historical data of the blockchain state.

In essence, it's a framework to modelize your data in such way that the library
knowns how to store this save this data at any block height.

#### Tablet

A `Tablet` in FluxDB is a set of rows grouped under a single "entity" forming a
logical set of data for this entity.

An example of this would be all users' balance for a given token. The `Tablet`
entity would be the contract's token while each row would be an account the row's
value the balance of this account at a given block height.

Using this information, FluxDB framework will be able to retrieve you the state
of all the balances for a given block height, even if all users modified their
balance at a different block height.

#### Singlet

A `Singlet` in FluxDB is a set of entry for a given state value written in such
way that it's possible to efficiently query the current state of the value as well
as querying the state at a given block height.

A `Singlet` is useful when for a single state value, you want the most efficient
way to retrieve the current value.

An example of this could be to retrieve current balance of a single user. By using
a `Singlet`, it will be much more efficient to retrieve the single user's balance
efficiently instead of using a `Tablet` that could requires retrieving the value for
a few thousand rows for example

### Usage

See the different protocol-specific `StreamingFast` binaries at https://github.com/streamingfast/streamingfast#protocols

Current `fluxdb` implementations:

* [EOSIO on StreamingFast](https://github.com/streamingfast/sf-eosio)
* [Ethereum on StreamingFast](https://github.com/streamingfast/sf-ethereum)

## Contributing

**Issues and PR in this repo related strictly to the core search engine.**

Report any protocol-specific issues in their
[respective repositories](https://github.com/streamingfast/streamingfast#protocols)

**Please first refer to the general
[StreamingFast contribution guide](https://github.com/streamingfast/streamingfast/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

This codebase uses unit tests extensively, please write and run tests.


## License

[Apache 2.0](LICENSE)