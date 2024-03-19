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

// Remove pass data and ophans
function trimPassDataAndOrphans(&$jsonData) {
    $passTagIds = [];
    $passNodeIds = [];
    $allTagIds = [];
    $allNodeIds = [];

    // Step 1: Identify "pass" messages and collect their related tags and nodes
    foreach ($jsonData['messages'] as $index => $message) {
        if ($message['type'] === 'pass') {
            $passTagIds = array_merge($passTagIds, $message['relatedTagIds']);
            $passNodeIds = array_merge($passNodeIds, $message['relatedNodeIds']);
            unset($jsonData['messages'][$index]); // Remove "pass" message
        } else {
            $allTagIds = array_merge($allTagIds, $message['relatedTagIds']);
            $allNodeIds = array_merge($allNodeIds, $message['relatedNodeIds']);
        }
    }

    // Re-index messages after removing "pass" messages
    $jsonData['messages'] = array_values($jsonData['messages']);

    // Step 2: Remove exclusive "pass" related tags and nodes
    // At this point, $allTagIds and $allNodeIds contain tags and nodes related to non-pass messages
    $jsonData['tags'] = array_filter($jsonData['tags'], function ($tag) use ($allTagIds) {
        return in_array($tag['tagId'], $allTagIds);
    });

    $jsonData['nodes'] = array_filter($jsonData['nodes'], function ($node) use ($allNodeIds) {
        return in_array($node['nodeId'], $allNodeIds);
    });

    // Step 3: Further trim tags and nodes that are not related to any messages
    // At this point, some tags or nodes may still not be associated with any remaining messages
    $relatedTagIds = [];
    $relatedNodeIds = [];

    foreach ($jsonData['messages'] as $message) {
        $relatedTagIds = array_merge($relatedTagIds, $message['relatedTagIds']);
        $relatedNodeIds = array_merge($relatedNodeIds, $message['relatedNodeIds']);
    }

    $jsonData['tags'] = array_filter($jsonData['tags'], function ($tag) use ($relatedTagIds) {
        return in_array($tag['tagId'], $relatedTagIds);
    });

    $jsonData['nodes'] = array_filter($jsonData['nodes'], function ($node) use ($relatedNodeIds) {
        return in_array($node['nodeId'], $relatedNodeIds);
    });

    // Re-index tags and nodes arrays after filtering
    $jsonData['tags'] = array_values($jsonData['tags']);
    $jsonData['nodes'] = array_values($jsonData['nodes']);
}

// Count Unique Items in JSON
function countUniqueItemsInJson(array $jsonData) {
    // Initialize sets to track unique items
    $uniqueMessages = [];
    $uniqueTags = [];
    $uniqueNodes = [];
    $totalRelatedTags = 0;
    $totalRelatedNodes = 0;

    // Count the URL (assuming there's only one URL in the structure)
    $totalURLs = isset($jsonData['url']) ? 1 : 0;

    // Count Unique Messages
    foreach ($jsonData['messages'] as $message) {
        $uniqueKey = md5($message['message'] . '|' . $message['type']);
        $uniqueMessages[$uniqueKey] = true; // Store in associative array to prevent duplicates
        
        // Sum up all related tags and nodes for each message
        $totalRelatedTags += count($message['relatedTagIds']);
        $totalRelatedNodes += count($message['relatedNodeIds']);
    }

    // Count Unique Tags
    foreach ($jsonData['tags'] as $tag) {
        $uniqueTags[$tag['tagId']] = true; // Assuming tagId is unique
    }

    // Count Unique Nodes
    foreach ($jsonData['nodes'] as $node) {
        $uniqueKey = md5($node['html'] . '|' . json_encode($node['targets']));
        $uniqueNodes[$uniqueKey] = true; // Use md5 hash of html and targets to identify uniqueness
    }

    // Echo counts
    echo "- " . count(array_filter($jsonData['messages'], fn($msg) => $msg['type'] === 'violation')) . " Unique Violation Messages in JSON\n";
    echo "- " . count(array_filter($jsonData['messages'], fn($msg) => $msg['type'] === 'pass')) . " Unique Pass Messages in JSON\n";
    echo "- " . count(array_filter($jsonData['messages'], fn($msg) => $msg['type'] === 'error')) . " Unique Error Messages in JSON\n";
    echo "- " . count($uniqueTags) . " Unique Tags in JSON\n";
    echo "- " . count($uniqueNodes) . " Unique Nodes in JSON\n";
    echo "- " . $totalURLs . " Total URLs in JSON\n";
    echo "- " . $totalRelatedTags . " Total Related Tags in All Messages\n";
    echo "- " . $totalRelatedNodes . " Total Related Nodes in All Messages\n";
}

// Add new messages.
function addNewMessages(PDO $pdo, array &$jsonData) {
    // Initialize counters for each message type with a similar structure but including unique checks.
    $uniqueMessages = [];
    $typeCounts = [
        'error' => ['all' => 0, 'new' => 0],
        'pass' => ['all' => 0, 'new' => 0],
        'violation' => ['all' => 0, 'new' => 0],
    ];

    foreach ($jsonData['messages'] as $key => $message) {
        // Create a unique key for each message based on its message and type.
        $uniqueKey = md5($message['message'] . '|' . $message['type']);

        // Only proceed if this is a unique message.
        if (!isset($uniqueMessages[$uniqueKey])) {
            $uniqueMessages[$uniqueKey] = true; // Mark this message as added.
            
            // Attempt to fetch an existing message ID from the database.
            $stmt = $pdo->prepare("SELECT message_id FROM messages WHERE message = :message AND message_type = :type");
            $stmt->execute(['message' => $message['message'], 'type' => $message['type']]);
            $messageId = $stmt->fetchColumn();
            
            // Increment counters.
            $typeCounts[$message['type']]['all']++;

            if (!$messageId) {
                // If it's new to the database, insert it.
                $insert = $pdo->prepare("INSERT INTO messages (message, message_type) VALUES (:message, :type)");
                $insert->execute(['message' => $message['message'], 'type' => $message['type']]);
                $messageId = $pdo->lastInsertId();

                $typeCounts[$message['type']]['new']++;
            }

            $jsonData['messages'][$key]['messageId'] = $messageId;
        }
    }

    // Log counts for each type.
    foreach ($typeCounts as $type => $counts) {
        echo "- Added {$counts['new']} New $type messages ({$counts['all']} Total)\n";
    }
}

// Process url.
function processUrl(PDO $pdo, array &$jsonData, $property_id) {
    $url = $jsonData['url'];
    $stmt = $pdo->prepare("SELECT url_id FROM urls WHERE url = :url AND url_property_id = :property_id");
    $stmt->execute(['url' => $url, 'property_id' => $property_id]);

    // Start for logging.
    $logging_message = '';

    // Give an existing id to an exisiting url.
    $urlId = $stmt->fetchColumn();

    if (!$urlId) {
        $insert = $pdo->prepare("INSERT INTO urls (url, url_property_id) VALUES (:url, :property_id)");
        $insert->execute(['url' => $url, 'property_id' => $property_id]);

        // Give a new id to a new url.
        $urlId = $pdo->lastInsertId();

        // Add logging message
        $logging_message = "New";
    }else{
        $logging_message = "Existing";
    }

    // Update tag in JSON
    $jsonData['urlId'] = $urlId;

    // Log count.
    echo "- Processed URL \"$url\" ($logging_message)\n";

}

// Add new tags.
function addNewTags(PDO $pdo, array &$jsonData) {
    $tagIdMap = []; // To map old tag IDs to new ones

    // Start counters for logging.
    $allItemsCounter = 0;
    $newItemsCounter = 0;

    foreach ($jsonData['tags'] as $key => $tag) {

        // Count all items.
        $allItemsCounter++;

        // See if tag alread exists
        $stmt = $pdo->prepare("SELECT tag_id FROM tags WHERE tag = :tag");
        $stmt->execute(['tag' => $tag['tag']]);
        $tagId = $stmt->fetchColumn();

        // Add tags that don't exist
        if (!$tagId) {
            $insert = $pdo->prepare("INSERT INTO tags (tag) VALUES (:tag)");
            $insert->execute(['tag' => $tag['tag']]);
            $tagId = $pdo->lastInsertId();

            // Count new item.
            $newItemsCounter++;

        }

        // Update tag ids in JSON.
        $tagIdMap[$tag['tagId']] = $tagId;
        $jsonData['tags'][$key]['tagId'] = $tagId;

    }

    // Log count.
    echo "- Added $newItemsCounter New Tags($allItemsCounter Total)\n";

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

// Add new nodes.
function addNewNodes(PDO $pdo, array &$jsonData) {
    $nodeIdMap = []; // To map old node IDs to new ones

    // Start counters for logging.
    $allItemsCounter = 0;
    $newItemsCounter = 0;

    foreach ($jsonData['nodes'] as $key => $node) {

        // Count all items.
        $allItemsCounter++;

        // See if node aleady exists.
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

            // Count new item.
            $newItemsCounter++;

        }

        $nodeIdMap[$node['nodeId']] = $nodeId;
        $jsonData['nodes'][$key]['nodeId'] = $nodeId;
    }

    // Log count.
    echo "- Added $newItemsCounter New Nodes($allItemsCounter Total)\n";

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

// Relate messages to URLS
function relateMessagesToUrl(PDO $pdo, array $jsonData, $property_id){

    // Start counters for logging.
    $allItemsCounter = 0;

    $urlId = $jsonData['urlId']; // Set in processUrl

    foreach ($jsonData['messages'] as $message) {
        // Relate message to URL (Assuming one URL per message from your JSON structure)
        $insertUrl = $pdo->prepare("INSERT IGNORE INTO message_urls (message_id, url_id) VALUES (:message_id, :url_id)");
        $insertUrl->execute(['message_id' => $message['messageId'], 'url_id' => $urlId]);

        // Count
        $allItemsCounter++;
    }

    // Log
    echo "- Related Messages to URL ID $urlId\n";

}

// Relate Messages to Nodes
function relateMessagesToNodes(PDO $pdo, array $jsonData) {
    $nodesToRemoveGlobally = [];

    // Start counters for logging.
    $newItemsCounter = 0;
    $deletedItemsCounter = 0;

    foreach ($jsonData['messages'] as $message) {
        $messageId = $message['messageId'];

        // Fetch existing node relations
        $existingNodeIdsStmt = $pdo->prepare("SELECT node_id FROM message_nodes WHERE message_id = :message_id");
        $existingNodeIdsStmt->execute(['message_id' => $messageId]);
        $existingNodeIds = $existingNodeIdsStmt->fetchAll(PDO::FETCH_COLUMN, 0);

        // Determine nodes to add or remove
        $nodesToAdd = array_diff($message['relatedNodeIds'], $existingNodeIds);
        $nodesToRemove = array_diff($existingNodeIds, $message['relatedNodeIds']);

        // Add new node relations
        foreach ($nodesToAdd as $nodeId) {
            $insertNode = $pdo->prepare("INSERT INTO message_nodes (message_id, node_id) VALUES (:message_id, :node_id)");
            $insertNode->execute(['message_id' => $messageId, 'node_id' => $nodeId]);

            // Count new items.
            $newItemsCounter++;

        }

        // Remove outdated node relations
        foreach ($nodesToRemove as $nodeId) {
            $deleteNode = $pdo->prepare("DELETE FROM message_nodes WHERE message_id = :message_id AND node_id = :node_id");
            $deleteNode->execute(['message_id' => $messageId, 'node_id' => $nodeId]);

            // Count deleted items.
            $deletedItemsCounter++;

            // Accumulate ndoes to be removed globally
            if (!in_array($nodeId, $nodesToRemoveGlobally))
                $nodesToRemoveGlobally[] = $nodeId;

        }

    }

    // Log
    echo "- Added $newItemsCounter and Deleted $deletedItemsCounter Nodes\n";

    // Return deleted node Ids
    return $nodesToRemoveGlobally;

}

function relateMessagesToTags(PDO $pdo, array $jsonData) {
    $tagsToRemoveGlobally = [];

    // Start counters for logging.
    $newItemsCounter = 0;
    $deletedItemsCounter = 0;

    foreach ($jsonData['messages'] as $message) {
        $messageId = $message['messageId'];

        // Fetch existing tag relations
        $existingTagIdsStmt = $pdo->prepare("SELECT tag_id FROM message_tags WHERE message_id = :message_id");
        $existingTagIdsStmt->execute(['message_id' => $messageId]);
        $existingTagIds = $existingTagIdsStmt->fetchAll(PDO::FETCH_COLUMN, 0);

        // Determine tags to add or remove
        $tagsToAdd = array_diff($message['relatedTagIds'], $existingTagIds);
        $tagsToRemove = array_diff($existingTagIds, $message['relatedTagIds']);

        // Add new tag relations
        foreach ($tagsToAdd as $tagId) {
            $insertTag = $pdo->prepare("INSERT INTO message_tags (message_id, tag_id) VALUES (:message_id, :tag_id)");
            $insertTag->execute(['message_id' => $messageId, 'tag_id' => $tagId]);

            // Count new items.
            $newItemsCounter++;

        }

        // Remove outdated tag relations
        foreach ($tagsToRemove as $tagId) {
            $deleteTag = $pdo->prepare("DELETE FROM message_tags WHERE message_id = :message_id AND tag_id = :tag_id");
            $deleteTag->execute(['message_id' => $messageId, 'tag_id' => $tagId]);

            // Count deleted items.
            $deletedItemsCounter++;

            // Accumulate tags to be removed globally
            if (!in_array($tagId, $tagsToRemoveGlobally))
                $tagsToRemoveGlobally[] = $tagId;

        }

    }

    // Log
    echo "- Added $newItemsCounter and Deleted $deletedItemsCounter Tags\n";

    // Return the list of tag IDs to remove
    return $tagsToRemoveGlobally;

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

    // Counters for different node statuses
    $equalifiedNodesCount = 0;
    $unequalifiedNodesCount = 0;

    // Get all node_ids associated with the URL and property_id from node_urls join.
    $stmt = $pdo->prepare("SELECT n.node_id, n.node_equalified
        FROM nodes n
        JOIN node_urls nu ON n.node_id = nu.node_id
        JOIN urls u ON nu.url_id = u.url_id
        WHERE u.url_id = :url_id AND u.url_property_id = :property_id");
    $stmt->execute(['url_id' => $urlId, 'property_id' => $property_id]);
    $dbNodes = $stmt->fetchAll(PDO::FETCH_ASSOC);

    // Convert JSON nodeIds to a simple array for comparison
    $jsonNodeIds = array_map(function($node) {
        return $node['nodeId'];
    }, $jsonData['nodes']);

    foreach ($dbNodes as $dbNode) {
        $dbNodeId = $dbNode['node_id'];
        $isNodeEqualified = $dbNode['node_equalified'];

        // Check for status change or first-time equalification
        if (!in_array($dbNodeId, $jsonNodeIds) && !$isNodeEqualified) {
            // Node is now equalified but wasn't before
            $updateStmt = $pdo->prepare("UPDATE nodes SET node_equalified = 1 WHERE node_id = :node_id");
            $updateStmt->execute(['node_id' => $dbNodeId]);
            $equalifiedNodesCount++;

            // Add entry to node_updates
            $insert = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, 1, NOW())");
            $insert->execute(['node_id' => $dbNodeId]);
        } elseif (in_array($dbNodeId, $jsonNodeIds) && $isNodeEqualified) {
            // Node is present in JSON but was previously marked as equalified
            $updateStmt = $pdo->prepare("UPDATE nodes SET node_equalified = 0 WHERE node_id = :node_id");
            $updateStmt->execute(['node_id' => $dbNodeId]);
            $unequalifiedNodesCount++;

            // Add entry to node_updates
            $insert = $pdo->prepare("INSERT INTO node_updates (node_id, node_equalified, update_date) VALUES (:node_id, 0, NOW())");
            $insert->execute(['node_id' => $dbNodeId]);
        }
    }

    // Echo out the totals
    echo "- Nodes Equalified: $equalifiedNodesCount\n";
    echo "- Nodes Un-equalified: $unequalifiedNodesCount\n";
}

// Delete tags that were removed and are not in use by other messages.
function deleteUnusedTags(PDO $pdo, array $tagsToRemove) {
    if (empty($tagsToRemove)) return;

    $deletedCount = 0;
    foreach ($tagsToRemove as $tagId) {
        // Check if this tag is still used in any message
        $checkStmt = $pdo->prepare("SELECT COUNT(*) FROM message_tags WHERE tag_id = ?");
        $checkStmt->execute([$tagId]);
        if ($checkStmt->fetchColumn() == 0) {
            // Tag is not used anymore, delete it
            $deleteStmt = $pdo->prepare("DELETE FROM tags WHERE tag_id = ?");
            $deleteStmt->execute([$tagId]);
            $deletedCount++;
        }
    }

    echo "- Deleted " . $deletedCount . " Unused Tags\n";
}

try {
    $pdo->beginTransaction();

    // Log start
    echo "Starting process_scans.php\n";

    // We aren't interested in pass data, since we'll 
    // get it when violations or errors are equalified.
    // We also want to get rid of any ophans because
    // we know a node is equalified when it isn't in
    // the JSON. We also remove tags so we don't fill
    // the DB with unnessary tags.
    trimPassDataAndOrphans($jsonData);

    // Count unique items in JSON to compare, since
    // all json counts should add up to added counts.
    countUniqueItemsInJson($jsonData);

    // Process each component
    addNewMessages($pdo, $jsonData);
    processUrl($pdo, $jsonData, $property_id);
    addNewTags($pdo, $jsonData);
    addNewNodes($pdo, $jsonData);

    // Relate content.
    relateMessagesToUrl($pdo, $jsonData, $property_id);
    $tagsToRemove = relateMessagesToTags($pdo, $jsonData);
    $nodesToRemove = relateMessagesToNodes($pdo, $jsonData);
    relateNodesToUrls($pdo, $jsonData);

    // Clean items.
    deleteUnusedTags($pdo, $tagsToRemove);
    
    // After all processing is done, compare and update nodes
    compareAndUpdateNodes($pdo, $jsonData, $property_id);

    $pdo->commit();

    // Log end
    echo "Ending process_scans.php\n";

} catch (Exception $e) {
    $pdo->rollBack();
    echo "Error: " . $e->getMessage();
}
?>
