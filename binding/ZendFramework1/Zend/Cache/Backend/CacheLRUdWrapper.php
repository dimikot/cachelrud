<?php
/**
 * Zend_Cache_Backend_CacheLRUdWrapper: support for CacheLRUd daemon.
 * 
 * This wrapper sends UDP messages to CacheLRUDaemon on each group of
 * cache id reading/writing. The real sending is performed on script
 * shutdown to pack all requests to the same message and save bandwidth.
 */
require_once "Zend/Cache/Backend/ExtendedInterface.php";

class Zend_Cache_Backend_CacheLRUdWrapper implements Zend_Cache_Backend_ExtendedInterface
{
    const DEFAULT_PORT = 43521;
    const MAX_MSG_SIZE = 10240;

    /**
     * @var Zend_Cache_Backend_ExtendedInterface
     */
    private $_backend = null;

    /**
     * @var string
     */
    private $_collectionName = null;

    /**
     * @var string
     */
    private $_host = null;

    /**
     * @var int
     */
    private $_port = null;

    /**
     * @var callback
     */
    private $_logger = null;

    /**
     * @var array
     */
    private $_buf = array();

    /**
     * @param Zend_Cache_Backend_Interface $backend
     * @param string $collectionName
     * @param string $host
     * @param int $port
     * @param callback $logger  Accepts 2 arguments: function(string $message, float $exec_time_seconds)
     */
    public function __construct($backend, $collectionName, $host, $port = null, $logger = null)
    {
        $this->_backend = $backend;
        $this->_collectionName = $collectionName;
        $this->_host = $host;
        $this->_port = $port? $port : self::DEFAULT_PORT;
        $this->_logger = $logger;
        register_shutdown_function(array($this, 'flushHits'));
    }

    /**
     * @return void
     */
    public function flushHits()
    {
        $t0 = microtime(true);
        $socket = stream_socket_client("udp://{$this->_host}:{$this->_port}");
        if (!$socket) {
            return;
        }
        stream_set_blocking($socket, 0); // possibly useless?
        $this->_log("UDP client socket created", microtime(true) - $t0);
        $lines = "";
        foreach ($this->_buf as $id => $dummy) {
            $line = $this->_collectionName . ":" . $id . "\n";
            if (strlen($lines) + strlen($line) > self::MAX_MSG_SIZE) {
                $this->_socketWrite($socket, $lines);
                $lines = "";
            }
            $lines .= $line;
        }
        $this->_socketWrite($socket, $lines);
        $this->_buf = array();
    }

    private function _log($msg, $dt)
    {
        if (!$this->_logger) {
            return;
        }
        call_user_func($this->_logger, get_class($this) . ": " . $msg . " (took " . intval($dt * 1000) . " ms)", $dt);
    }

    private function _socketWrite($socket, $data)
    {
        if (!strlen($data)) {
            return;
        }
        $t0 = microtime(true);
        fwrite($socket, $data);
        $this->_log(
            sprintf(
                "sending UDP packet with %d bytes to %s:%s",
                strlen($data), $this->_host, $this->_port
            ),
            microtime(true) - $t0
        );
    }

    public function setDirectives($directives)
    {
        return $this->_backend->setDirectives($directives);
    }

    public function load($id, $doNotTestCacheValidity = false)
    {
        $result = $this->_backend->load($id, $doNotTestCacheValidity);
        if ($result !== false) {
            $this->_buf[$id] = time();
        }
        return $result;
    }
    
    
    public function test($id)
    {
        $result = $this->_backend->test($id);
        if ($result !== false) {
            $this->_buf[$id] = time();
        }
        return $result;
    }
    
    
    public function save($data, $id, $tags = array(), $specificLifetime = false)
    {
        $this->_buf[$id] = time();
        return $this->_backend->save($data, $id, $tags, $specificLifetime);
    }
    
    
    public function remove($id)
    {
        return $this->_backend->remove($id);
    }
    
    
    public function clean($mode = Zend_Cache::CLEANING_MODE_ALL, $tags = array())
    {
        return $this->_backend->clean($mode, $tags);
    }

    public function getIds()
    {
        return $this->_backend->getIds();
    }

    public function getTags()
    {
        return $this->_backend->getTags();
    }

    public function getIdsMatchingTags($tags = array())
    {
        return $this->_backend->getIdsMatchingTags($tags);
    }

    public function getIdsNotMatchingTags($tags = array())
    {
        return $this->_backend->getIdsNotMatchingTags($tags);
    }

    public function getIdsMatchingAnyTags($tags = array())
    {
        return $this->_backend->getIdsMatchingAnyTags($tags);
    }

    public function getFillingPercentage()
    {
        return $this->_backend->getFillingPercentage();
    }

    public function getMetadatas($id)
    {
        return $this->_backend->getMetadatas($id);
    }

    public function touch($id, $extraLifetime)
    {
        return $this->_backend->touch($id, $extraLifetime);
    }

    public function getCapabilities()
    {
        return $this->_backend->getCapabilities();
    }
}
