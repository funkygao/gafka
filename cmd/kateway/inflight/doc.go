// Package inflight provides storage for manipulating inflight
// message offsets.
//
//  server               client
//    |                    |
//    |               Sub  |
//    |<-------------------|
//    |                    |
//    | Ok/TakeOff(1)      |
//    |------------------->|
//    |                    |
//    |        Sub/Land(1) |
//    |<-------------------|
//    |                    |
//    | Ok/TakeOff(2)      |
//    |------------------->|
//    |                    |
//    |        Sub/Land(2) |
//    |<-------------------|
//    |                    |
package inflight
