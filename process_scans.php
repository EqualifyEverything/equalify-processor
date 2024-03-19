<?php
// This file is designed to be run from command line
// so we can do things like trigger via CRON.
if(!defined('__ROOT__'))
    define('__ROOT__', dirname(dirname(__FILE__)));

// Get DB and global info.
require_once(__ROOT__.'/init.php'); 

// Testing data
$property_id = 1; 
$jsonFilePath = __ROOT__.'/_dev/stream-sample.json';
$jsonData = json_decode(file_get_contents($jsonFilePath), true);

// Process messages.
function processMessages(PDO $pdo, array &$jsonData) {
    
    // See if messages exist.
    foreach ($jsonData['messages'] as $key => $message) {
        $stmt = $pdo->prepare("SELECT message_id FROM messages WHERE message = :message AND message_type = :type");
        $stmt->execute(['message' => $message['message'], 'type' => $message['type']]);

        // Give an existing id to an exisiting message.
        $messageId = $stmt->fetchColumn();

        if (!$messageId) {
            $insert = $pdo->prepare("INSERT INTO messages (message, message_type) VALUES (:message, :type)");
            $insert->execute(['message' => $message['message'], 'type' => $message['type']]);

            // Give a new id to a new message.
            $messageId = $pdo->lastInsertId();

        }

        $jsonData['messages'][$key]['messageId'] = $messageId;
    }

}

// Process urls.
function processUrls(PDO $pdo, array &$jsonData, $property_id) {
    $url = $jsonData['url'];
    $stmt = $pdo->prepare("SELECT url_id FROM urls WHERE url = :url AND url_property_id = :property_id");
    $stmt->execute(['url' => $url, 'property_id' => $property_id]);

    // Give an existing id to an exisiting url.
    $urlId = $stmt->fetchColumn();

    if (!$urlId) {
        $insert = $pdo->prepare("INSERT INTO urls (url, url_property_id) VALUES (:url, :property_id)");
        $insert->execute(['url' => $url, 'property_id' => $property_id]);

        // Give a new id to a new url.
        $urlId = $pdo->lastInsertId();

    }

    $jsonData['urlId'] = $urlId;
}

// Process tags.
function processTags(PDO $pdo, array &$jsonData) {
    $tagIdMap = []; // To map old tag IDs to new ones

    foreach ($jsonData['tags'] as $key => $tag) {
        $stmt = $pdo->prepare("SELECT tag_id FROM tags WHERE tag = :tag");
        $stmt->execute(['tag' => $tag['tag']]);
        $tagId = $stmt->fetchColumn();

        if (!$tagId) {
            $insert = $pdo->prepare("INSERT INTO tags (tag) VALUES (:tag)");
            $insert->execute(['tag' => $tag['tag']]);
            $tagId = $pdo->lastInsertId();
        }

        $tagIdMap[$tag['tagId']] = $tagId;
        $jsonData['tags'][$key]['tagId'] = $tagId;
    }

    // Update relatedTagIds in messages
    foreach ($jsonData['messages'] as &$message) {
        if (!empty($message['relatedTagIds'])) {
            foreach ($message['relatedTagIds'] as &$relatedTagId) {
                if (isset($tagIdMap[$relatedTagId])) {
                    $relatedTagId = $tagIdMap[$relatedTagId];
                }
            }
        }
    }
}

// Process nodes.
function processNodes(PDO $pdo, array &$jsonData) {
    $nodeIdMap = []; // To map old node IDs to new ones

    foreach ($jsonData['nodes'] as $key => $node) {
        $stmt = $pdo->prepare("SELECT node_id FROM nodes WHERE node_html = :node_html");
        $stmt->execute(['node_html' => $node['html']]);
        $nodeId = $stmt->fetchColumn();

        if (!$nodeId) {
            $insert = $pdo->prepare("INSERT INTO nodes (node_html, node_targets) VALUES (:node_html, :node_targets)");
            $insert->execute(['node_html' => $node['html'], 'node_targets' => json_encode($node['targets'])]);
            $nodeId = $pdo->lastInsertId();

            // Since this is a new node, add it to node_updates marking as not equalified
            $insertUpdate = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, 0, NOW())");
            $insertUpdate->execute(['node_id' => $nodeId]);
        }

        $nodeIdMap[$node['nodeId']] = $nodeId;
        $jsonData['nodes'][$key]['nodeId'] = $nodeId;
    }

    // Update relatedNodeIds in messages
    foreach ($jsonData['messages'] as &$message) {
        if (!empty($message['relatedNodeIds'])) {
            foreach ($message['relatedNodeIds'] as &$relatedNodeId) {
                if (isset($nodeIdMap[$relatedNodeId])) {
                    $relatedNodeId = $nodeIdMap[$relatedNodeId];
                }
            }
        }
    }
}

// Relate messages.
function relateMessages(PDO $pdo, array $jsonData, $property_id) {
    foreach ($jsonData['messages'] as $message) {
        $messageId = $message['messageId'];

        // Relate message to nodes
        if (!empty($message['relatedNodeIds'])) {
            foreach ($message['relatedNodeIds'] as $nodeId) {
                $insert = $pdo->prepare("INSERT IGNORE INTO message_nodes (message_id, node_id) VALUES (:message_id, :node_id)");
                $insert->execute(['message_id' => $messageId, 'node_id' => $nodeId]);
            }
        }

        // Relate message to tags
        if (!empty($message['relatedTagIds'])) {
            foreach ($message['relatedTagIds'] as $tagId) {
                $insert = $pdo->prepare("INSERT IGNORE INTO message_tags (message_id, tag_id) VALUES (:message_id, :tag_id)");
                $insert->execute(['message_id' => $messageId, 'tag_id' => $tagId]);
            }
        }

        // Since a message might relate to multiple URLs or nodes which in turn could be related to a property,
        // We ensure a message is linked to the property it's related to, via the URL in JSON
        $insertProperty = $pdo->prepare("INSERT IGNORE INTO message_properties (message_id, property_id) VALUES (:message_id, :property_id)");
        $insertProperty->execute(['message_id' => $messageId, 'property_id' => $property_id]);

        // Relate message to URL (Assuming one URL per message from your JSON structure)
        $urlId = $jsonData['urlId']; // This is set from processUrls
        $insertUrl = $pdo->prepare("INSERT IGNORE INTO message_urls (message_id, url_id) VALUES (:message_id, :url_id)");
        $insertUrl->execute(['message_id' => $messageId, 'url_id' => $urlId]);
    }
}

// Relate nodes to URLs.
function relateNodesToUrls(PDO $pdo, array $jsonData) {
    $urlId = $jsonData['urlId']; // Obtained from processUrls

    // Check and insert node and URL relations
    foreach ($jsonData['nodes'] as $node) {
        $nodeId = $node['nodeId'];

        // Insert relation into node_urls
        $insert = $pdo->prepare("INSERT IGNORE INTO node_urls (node_id, url_id) VALUES (:node_id, :url_id)");
        $insert->execute(['node_id' => $nodeId, 'url_id' => $urlId]);
    }
}

// Compare and update nodes.
function compareAndUpdateNodes(PDO $pdo, array $jsonData, $property_id) {
    $urlId = $jsonData['urlId'];
    
    // Get all node_ids associated with the URL and property_id from node_urls join.
    $stmt = $pdo->prepare("SELECT n.node_id FROM nodes n 
                           JOIN node_urls nu ON n.node_id = nu.node_id 
                           WHERE nu.url_id = :url_id");
    $stmt->execute(['url_id' => $urlId]);
    $dbNodeIds = $stmt->fetchAll(PDO::FETCH_COLUMN, 0);

    // Convert JSON nodeIds to a simple array for comparison
    $jsonNodeIds = array_map(function($node) {
        return $node['nodeId'];
    }, $jsonData['nodes']);

    // Check for new nodes in JSON
    foreach ($jsonNodeIds as $jsonNodeId) {
        if (!in_array($jsonNodeId, $dbNodeIds)) {
            // This is a new node. Add it to node_updates with node_equalified = false.
            $insert = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, :node_equalified, NOW())");
            $insert->execute(['node_id' => $jsonNodeId, 'node_equalified' => 0]);
        }
    }

    // Check for existing nodes and update if necessary
    foreach ($dbNodeIds as $dbNodeId) {
        // If node exists in DB but not in JSON, it's equalified
        if (!in_array($dbNodeId, $jsonNodeIds)) {
            // Insert a new row into node_updates with node_equalified set to true
            $insert = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, :node_equalified, NOW())");
            $insert->execute(['node_id' => $dbNodeId, 'node_equalified' => 1]);
        } else {
            // Check the latest entry in node_updates for this node
            $stmt = $pdo->prepare("SELECT node_equalified FROM node_updates WHERE node_id = :node_id ORDER BY update_date DESC LIMIT 1");
            $stmt->execute(['node_id' => $dbNodeId]);
            $lastStatus = $stmt->fetchColumn();
            
            // If the last status is true (equalified), insert a new entry with false
            if ($lastStatus) {
                $insert = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, :node_equalified, NOW())");
                $insert->execute(['node_id' => $dbNodeId, 'node_equalified' => 0]);
            } 
        }
    }
}

try {
    $pdo->beginTransaction();

    // Process each component
    processMessages($pdo, $jsonData);
    processUrls($pdo, $jsonData, $property_id);
    processTags($pdo, $jsonData);
    processNodes($pdo, $jsonData);

    // Relate content.
    relateMessages($pdo, $jsonData, $property_id);
    relateNodesToUrls($pdo, $jsonData);

    // After all processing is done, compare and update nodes
    compareAndUpdateNodes($pdo, $jsonData, $property_id);

    $pdo->commit();
} catch (Exception $e) {
    $pdo->rollBack();
    echo "Error: " . $e->getMessage();
}
?>