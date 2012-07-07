<?php
class SQSSource extends DataSource {
    public $description = "Amazon Simple Queue Service (SQS)";
    
    public $config = array(
        'key' => '',
        'secret' => ''
    );
    
    public $columns = array(
        'boolean' => array('name' => 'boolean'),
        'string' => array('name' => 'varchar'),
        'text' => array('name' => 'text'),
        'integer' => array('name' => 'integer', 'format' => null, 'formatter' => 'intval'),
        'float' => array('name' => 'float', 'format' => null, 'formatter' => 'floatval'),
        'datetime' => array('name' => 'datetime'),
    );
        
    public function __construct($config) {
        parent::__construct($config);
        
        CFCredentials::set(array(
            
            // Credentials for the development environment.
            'development' => array(

                // Amazon Web Services Key. Found in the AWS Security Credentials. You can also pass
                // this value as the first parameter to a service constructor.
                'key' => $config['key'],

                // Amazon Web Services Secret Key. Found in the AWS Security Credentials. You can also
                // pass this value as the second parameter to a service constructor.
                'secret' => $config['secret'],

                // This option allows you to configure a preferred storage type to use for caching by
                // default. This can be changed later using the set_cache_config() method.
                //
                // Valid values are: `apc`, `xcache`, or a file system path such as `./cache` or
                // `/tmp/cache/`.
                'default_cache_config' => '',

                // Determines which Cerificate Authority file to use.
                //
                // A value of boolean `false` will use the Certificate Authority file available on the
                // system. A value of boolean `true` will use the Certificate Authority provided by the
                // SDK. Passing a file system path to a Certificate Authority file (chmodded to `0755`)
                // will use that.
                //
                // Leave this set to `false` if you're not sure.
                'certificate_authority' => false
            ),

            // Specify a default credential set to use if there are more than one.
            '@default' => 'development'
        ));
    }
    
    public function listSources() {
        return null;
    }
    
    public function describe(Model $Model) {
        return $Model->_schema;
    }
    
    /**
     * calculate() is for determining how we will count the records and is
     * required to get ``update()`` and ``delete()`` to work.
     *
     * We don't count the records here but return a string to be passed to
     * ``read()`` which will do the actual counting. The easiest way is to just
     * return the string 'COUNT' and check for it in ``read()`` where
     * ``$data['fields'] == 'COUNT'``.
     */
    public function calculate(Model $Model, $func, $params = array()) {
        return 'COUNT';
    }
    
    
    public function create(Model $Model, $fields = array(), $values = array()) {
        $data = array_combine($fields, $values);

        return $this->_sendMessage($Model->queueURL, json_encode($data));
    }
    
    public function update(Model $Model, $fields = array(), $values = array()) {
        return $this->create($Model, $fields, $values);
    }
    
    public function read(Model $Model, $data = array()) {
        if ($data['fields'] == 'COUNT') {
            return array(array(array('count' => $this->_getQueueSize($Model->queueURL))));
        }
        
        if (isset($data['limit']) && !empty($data['limit'])) {
            $options['MaxNumberOfMessages'] = $data['limit'];
        }

        $response = $this->_receiveMessage($Model->queueURL, $options);

        if ($response === false) return false;
        else {
            $response = $response->to_stdClass();
            $messages = $response->ReceiveMessageResult->Message;
            
            $results = array();
            if (is_array($messages)) {
                foreach ($messages as $message) {
                    $row = array($Model->alias => get_object_vars(json_decode($message->Body)));
                    $row[$Model->alias]['message_id'] = $message->MessageId;
                    $row[$Model->alias]['receipt_handle'] = $message->ReceiptHandle;
                    $results[] = $row;
                }
            }
            else {
                $message = $messages;
                $row = array($Model->alias => get_object_vars(json_decode($message->Body)));
                $row[$Model->alias]['message_id'] = $message->MessageId;
                $row[$Model->alias]['receipt_handle'] = $message->ReceiptHandle;
                $results[] = $row;
            }
            
            return $results;
        }
    }
    
    public function delete(Model $Model, $conditions = null) {
        $receiptHandle = null;
        
        if (isset($conditions[$Model->alias.'.receipt_handle']) && !empty($conditions[$Model->alias.'.receipt_handle'])) {
            $receiptHandle = $conditions[$Model->alias.'.receipt_handle'];
        }
        elseif (isset($conditions[$Model->alias.'.id']) && !empty($conditions[$Model->alias.'.id'])) {
            $receiptHandle = $conditions[$Model->alias.'.id'];
        }
        else return false;
        
        return $this->_deleteMessage($Model->queueURL, $receiptHandle);
    }

/**
 * Wrappers of AmazonSQS functions
 */    
    protected function _sendMessage($queueURL, $message) {
        if (!is_string($message)) return false;
        
        $sqs = new AmazonSQS();
        $response = $sqs->send_message($queueURL, $message);
        
        if (!$response->isOK()) {
            $this->log($response->body, 'sqs');
            return false;
        }
        
        return true;
    }
    
    protected function _receiveMessage($queueURL, $opt = null) {
        $sqs = new AmazonSQS();
        $response = $sqs->receive_message($queueURL, $opt);
        
        if (!$response->isOK()) {
            $this->log($response->body, 'sqs');
            return false;
        }
        
        return $response->body;
    }
    
    protected function _getQueueSize($queueURL) {
        $sqs = new AmazonSQS();
        return $sqs->get_queue_size($queueURL);
    }
    
    protected function _deleteMessage($queueURL, $receiptHandle, $opt = null) {
        $sqs = new AmazonSQS();
        $response = $sqs->delete_message($queueURL, $receiptHandle, $opt);
                
        if (!$response->isOK()) {
            $this->log($response->body, 'sqs');
            return false;
        }
        
        return true;
    }
}