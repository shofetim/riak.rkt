#lang racket/base

;; Settings
(define host "127.0.0.1")
(define port "8098")

;; Default values for
;; r: Number of nodes that must respond to a read
;; w: Number of nodes that must respond to a write
;; dw: Number of nodes that must write to durable storage.
(define r 2)
(define w 2)
(define dw 2)

;; A unique client id for the vector clocks
(define client-id 
  (string-append 
   "rkt-" 
     (number->string (random 4294967087))))

(provide (all-defined-out))
