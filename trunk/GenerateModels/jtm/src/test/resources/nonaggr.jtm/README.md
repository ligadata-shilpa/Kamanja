https://docs.oracle.com/cd/E13157_01/wlevs/docs30/epl_guide/clauses.html

  [ INSERT INTO insert_into_def ]
    SELECT select_list
  { FROM stream_source_list /  MATCHING pattern_expression }
  [ WHERE search_conditions ]
  [ GROUP BY grouping_expression_list ]
  [ HAVING grouping_search_conditions ]
  [ ORDER BY order_by_expression_list ]
  [ OUTPUT output_specification ]
  
  RETAIN BATCH OF 30 SECONDS
  RETAIN 100 EVENTS
  RETAIN 30 SECONDS
  
Pulled matching examples from https://docs.oracle.com/cd/E13157_01/wlevs/docs30/epl_guide/overview.html that don't have aggregations and should apply to Kamanja

Finding Network Anomalies

A customer may be in the middle of a check-in when the terminal detects a hardware problem or when the network goes down. In that situation we want to alert a team member to help the customer. When the terminal detects a problem, it issues an OutOfOrder event. A pattern can find situations where the terminal indicates out-of-order and the customer is in the middle of the check-in process:

   SELECT ci.term 
  MATCHING ci:=Checkin FOLLOWED BY 
        ( OutOfOrder (term.id=ci.term.id) AND NOT
          (Cancelled (term.id=ci.term.id) OR 
           Completed (term.id=ci.term.id) ) WITHIN 3 MINUTES )
Each self-service terminal can publish any of the four events below.

 Checkin - Indicates a customer started a check-in dialogue.
 Cancelled - Indicates a customer cancelled a check-in dialogue.
 Completed - Indicates a customer completed a check-in dialogue.
 OutOfOrder - Indicates the terminal detected a hardware problem
 
All events provide information about the terminal that published the event, and a timestamp. The terminal information is held in a property named term and provides a terminal id. Because all events carry similar information, we model each event as a subtype to a base class TerminalEvent, which will provide the terminal information that all events share. This enables us to treat all terminal events polymorphically, which simplifies our queries by allowing us to treat derived event types just like their parent event types.

Detecting Absence of Event

Because Status events arrive in regular intervals of 60 seconds, we can make use of temporal pattern matching using the MATCHING clause to find events that did not arrive in time. We can use the WITHIN operator to keep a 65 second window to account for a possible delay in transmission or processing and the NOT operator to detect the absence of a Status event with a term.id equal to T1:

   SELECT 'terminal 1 is offline' 
  MATCHING NOT Status(term.id = 'T1') WITHIN 65 SECONDS
  OUTPUT FIRST EVERY 5 MINUTES
  
Combining Transaction Events
  
  In this example we compose an EPL statement to detect combined events in which each component of the transaction is present. We restrict the event matching to the events that arrived within the last 30 minutes. This statement uses the INSERT INTO syntax to generate a CombinedEvent event stream.
  
     INSERT INTO CombinedEvent(transactionId, customerId, supplierId, 
      latencyAC, latencyBC, latencyAB)
    SELECT C.transactionId, customerId, supplierId, 
      C.timestamp - A.timestamp, 
      C.timestamp - B.timestamp, 
      B.timestamp - A.timestamp 
    FROM TxnEventA A, TxnEventB B, TxnEventC C
    RETAIN 30 MINUTES
    WHERE A.transactionId = B.transactionId AND
      B.transactionId = C.transactionId
      
      
Finding Dropped Transaction Events

An OUTER JOIN allows us to detect a transaction that did not make it through all three events. When TxnEventA or TxnEventB events leave their respective time windows consisting of the last 30 minutes of events, EPL filters out rows in which no EventC row was found.

   SELECT * 
    FROM TxnEventA A 
         FULL OUTER JOIN TxnEventC C ON A.transactionId = C.transactionId
         FULL OUTER JOIN TxnEventB B ON B.transactionId = C.transactionId
    RETAIN 30 MINUTES
  WHERE C.transactionId is null      
  
Previous Event Per Group

The combination of the PREV function and the PARTITION BY clause returns the property value for a previous event in the given group.

For example, assume we want to obtain the price of the previous event of the same symbol as the current event.

The statement that follows solves this problem. It partitions the window on the symbol property over a time window of one minute. As a result, when the engine encounters a new symbol value that it hasn't seen before, it creates a new window specifically to hold events for that symbol. Consequently, the PREV function returns the previous event within the respective time window for that event's symbol value.

   SELECT PREV(1, price) AS prevPrice 
  FROM Trade RETAIN 1 MIN PARTITION BY symbol
    