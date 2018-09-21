package net.jr.deebee.util.path;

import java.util.*;

public abstract class AStarPathFinder<NodeType> {

    private class SearchNode {
        private NodeType data;
        private double gscore = Double.POSITIVE_INFINITY;
        private double fscore = Double.POSITIVE_INFINITY;
        private boolean closed = false;
        private boolean outOpenSet = true;
        private SearchNode cameFrom = null;
    }

    public double distanceBetween(NodeType n1, NodeType n2) {
        return 1.0;
    }

    public double heuristicCostEstimate(NodeType current, NodeType goal) {
        return Double.POSITIVE_INFINITY;
    }

    public abstract Iterable<NodeType> getNeighbors(NodeType n);

    public boolean isGoalReached(NodeType current, NodeType goal) {
        return current == goal;
    }

    private List<NodeType> reconstructPath(SearchNode searchNode) {
        List<NodeType> list = new ArrayList<>();
        SearchNode current = searchNode;
        while (current != null) {
            list.add(current.data);
            current = current.cameFrom;
        }
        Collections.reverse(list);
        return list;
    }

    public final List<NodeType> find(NodeType start, NodeType goal) {

        if (start == null) {
            throw new IllegalArgumentException();
        }

        if (isGoalReached(start, goal)) {
            return Arrays.asList(start);
        }

        Map<NodeType, SearchNode> searchNodes = new HashMap<NodeType, SearchNode>() {
            @Override
            public SearchNode get(Object key) {
                if (containsKey(key)) {
                    return super.get(key);
                } else {
                    SearchNode sn = new SearchNode();
                    sn.data = (NodeType) key;
                    put((NodeType) key, sn);
                    return sn;
                }
            }
        };
        SearchNode startNode = searchNodes.get(start);
        startNode.data = start;
        startNode.gscore = 0;
        startNode.fscore = heuristicCostEstimate(start, goal);
        PriorityQueue<SearchNode> openSet = new PriorityQueue<>(10, (o1, o2) -> (int) (o1.fscore - o2.fscore));
        openSet.add(startNode);
        while (!openSet.isEmpty()) {
            SearchNode current = openSet.remove();
            if (isGoalReached(current.data, goal)) {
                return reconstructPath(current);
            }
            current.outOpenSet = true;
            current.closed = true;
            for (NodeType n : getNeighbors(current.data)) {
                SearchNode neighbor = searchNodes.get(n);
                if (neighbor.closed) {
                    continue;
                }
                double tentativeGScore = current.gscore + distanceBetween(current.data, neighbor.data);
                if (tentativeGScore >= neighbor.gscore) {
                    continue;
                }
                neighbor.cameFrom = current;
                neighbor.gscore = tentativeGScore;
                neighbor.fscore = tentativeGScore + heuristicCostEstimate(neighbor.data, goal);
                if (neighbor.outOpenSet) {
                    neighbor.outOpenSet = false;
                    openSet.add(neighbor);
                }
            }
        }
        return null;
    }
}
