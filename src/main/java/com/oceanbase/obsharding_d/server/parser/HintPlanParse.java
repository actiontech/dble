/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.server.parser;

import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.plan.optimizer.HintPlanNode;
import com.oceanbase.obsharding_d.util.StringUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class HintPlanParse {
    private String or = "|";
    private String and = "&";
    private String comma = ",";
    private String leftParenthesis = "(";
    private String rightParenthesis = ")";
    private HashMap<String, Set<HintPlanNode>> dependMap = Maps.newHashMap();
    private HashMap<String, Set<HintPlanNode>> erMap = Maps.newHashMap();
    private LinkedHashMap<String, Type> hintPlanNodeMap = Maps.newLinkedHashMap();
    private HashMap<String, HintPlanNode> nodeMap = Maps.newHashMap();

    public void parse(String sql) {
        Node node = buildNode(sql);
        buildDependMap(node);
    }

    private Node buildNode(String sql) throws ConfigException {
        sql = sql.replaceAll("\\s+", "");
        LinkedList<Node> nodeStack = new LinkedList<>();
        // each part of the hint is resolved to Node
        List<Node> nodeList = getNodeList(sql);
        for (Node node : nodeList) {
            //parse according to the right parenthesis
            if (node.getData().equals(rightParenthesis)) {
                LinkedList<Node> childNodeStack = new LinkedList<>();
                while (!nodeStack.isEmpty() && !nodeStack.peek().getData().equals(leftParenthesis)) {
                    childNodeStack.push(nodeStack.pop());
                }
                if (nodeStack.isEmpty()) {
                    throw new ConfigException("hint miss parentheses");
                }
                nodeStack.pop();
                //parenthesis content parse,parsing by symbol priority
                if (!childNodeStack.isEmpty()) {
                    childNodeStack = buildChildNode(childNodeStack, comma);
                    childNodeStack = buildChildNode(childNodeStack, and);
                    childNodeStack = buildChildNode(childNodeStack, or);
                    nodeStack.push(childNodeStack.pop());
                }
            } else {
                nodeStack.push(node);
            }
        }
        if (nodeStack.size() > 1) {
            nodeStack = buildChildNode(nodeStack, comma, false);
            nodeStack = buildChildNode(nodeStack, and);
            nodeStack = buildChildNode(nodeStack, or);
        }
        Node peek = nodeStack.peek();
        if (nodeStack.size() > 1 || !(peek.isTable() || peek.getType() == Type.ER)) {
            throw new ConfigException("hint parse failure");
        }
        return nodeStack.pop();

    }

    private LinkedList<Node> buildChildNode(LinkedList<Node> nodeStack, String operator) {
        return buildChildNode(nodeStack, operator, true);
    }

    private LinkedList<Node> buildChildNode(LinkedList<Node> nodeStack, String operator, boolean isStack) {
        Node parentNode = null;
        Node preNode = null;
        LinkedList<Node> stack = new LinkedList<>();
        boolean right = false;
        while (!nodeStack.isEmpty()) {
            Node node;
            if (isStack) {
                node = nodeStack.pop();
            } else {
                node = nodeStack.removeLast();
            }

            if (StringUtil.equals(operator, node.getData())) {
                if (right && node.isTable() && completeNode(node)) {
                    setRightNode(preNode, node);
                    right = false;
                    continue;
                }
                if (stack.isEmpty() || completeNode(node)) {
                    stack.offer(node);
                    preNode = node;
                    continue;
                }
                if (Objects.nonNull(preNode) && preNode.isTable() && !completeNode(preNode)) {
                    throw new ConfigException("cannot contain continuous | or & symbols");
                }

                Node pre = stack.pollLast();
                if (Objects.isNull(node.getLeftNode())) {
                    setLeftNode(node, pre);
                    parentNode = node;
                    stack.offer(node);
                    preNode = node;
                    right = true;
                } else if (pre.isTable()) {
                    setRightNode(pre, node);
                    stack.offer(pre);
                    preNode = pre;
                    right = false;
                }
            } else if (right) {
                setRightNode(parentNode, node);
                right = false;
            } else {
                stack.offer(node);
                preNode = node;
            }
        }
        return stack;
    }

    private boolean completeNode(Node node) {
        return Objects.nonNull(node.getLeftNode()) && Objects.nonNull(node.getRightNode());
    }

    public List<Node> getNodeList(String sql) throws ConfigException {
        char[] chars = sql.toCharArray();
        StringBuilder nodeName = new StringBuilder();
        List<Node> nodeList = new ArrayList<>();
        StringBuilder er = new StringBuilder();
        boolean erRelation = false;
        for (char c : chars) {
            switch (c) {
                case ')':
                    String data = nodeName.toString();
                    if (erRelation) {
                        String erData = er.toString().replaceAll("\\(", "");
                        String[] split = erData.split(comma);
                        for (String tableName : split) {
                            if (Strings.isNullOrEmpty(tableName)) {
                                throw new ConfigException("er Relation need like (a,b,c)");
                            }
                            nodeNameDuplicateCheck(tableName);
                            nodeMap.putIfAbsent(tableName, HintPlanNode.of(tableName));
                            hintPlanNodeMap.put(tableName, Type.ER);
                        }
                        nodeList.add(new Node(erData, Type.ER));
                    } else if (!StringUtil.isBlank(data)) {
                        nodeList.add(new Node(data));
                        nodeNameDuplicateCheck(data);
                        nodeMap.putIfAbsent(data, HintPlanNode.of(data));
                        hintPlanNodeMap.put(data, Type.OR);
                    }
                    nodeName = new StringBuilder();
                    er = new StringBuilder();
                    erRelation = false;
                    nodeList.add(new Node(String.valueOf(c)));
                    break;
                case '(':
                    nodeList.add(new Node(String.valueOf(c)));
                    er.append(c);
                    break;
                case '|':
                    addNode(nodeName.toString(), nodeList, erRelation, Type.OR);
                    nodeList.add(new Node(String.valueOf(c), Type.OR));
                    nodeName = new StringBuilder();
                    er = new StringBuilder();
                    break;
                case '&':
                    addNode(nodeName.toString(), nodeList, erRelation, Type.AND);
                    nodeList.add(new Node(String.valueOf(c), Type.AND));
                    nodeName = new StringBuilder();
                    er = new StringBuilder();
                    break;
                case ',':
                    erRelation = true;
                    if (nodeList.isEmpty()) {
                        throw new ConfigException("hint parse failure");
                    }
                    if (!er.toString().startsWith(leftParenthesis)) {
                        throw new ConfigException("er Relation need like (a,b,c)");
                    }
                    nodeName.append(c);
                    er.append(c);
                    break;
                default:
                    nodeName.append(c);
                    er.append(c);
            }
        }
        String lastTable = nodeName.toString();
        if (!StringUtil.isBlank(lastTable)) {
            nodeList.add(new Node(lastTable));
            nodeNameDuplicateCheck(lastTable);
            nodeMap.putIfAbsent(lastTable, HintPlanNode.of(lastTable));
            hintPlanNodeMap.put(lastTable, Type.Other);
        }
        return nodeList;
    }

    @NotNull
    private void addNode(String nodeName, List<Node> nodeList, boolean erRelation, Type type) throws ConfigException {
        nodeNameDuplicateCheck(nodeName);
        if (erRelation) {
            throw new ConfigException("er Relation need like (a,b,c)");
        }
        if (nodeName.length() == 0) {
            return;
        }

        nodeMap.putIfAbsent(nodeName, HintPlanNode.of(nodeName));
        //table has dependencies will be added to the end
        hintPlanNodeMap.put(nodeName, type);
        nodeList.add(new Node(nodeName));
    }

    private void nodeNameDuplicateCheck(String nodeName) {
        if (nodeMap.containsKey(nodeName)) {
            throw new ConfigException("duplicate alias exist in the hint plan");
        }
    }


    private void buildDependMap(Node root) {
        LinkedList<Node> queue = new LinkedList<>();
        queue.offer(root);
        HashMap<Node, Node> map = Maps.newHashMap();

        while (!queue.isEmpty()) {
            Node node = queue.poll();
            Node leftNode = node.getLeftNode();
            Node rightNode = node.getRightNode();
            if (node.getType() == Type.AND) {
                map.put(rightNode, leftNode);
                buildDependMap(rightNode);
                Set<HintPlanNode> rightSet = buildTables(rightNode, Sets.newLinkedHashSet());
                Set<HintPlanNode> leftSet = buildTables(leftNode, Sets.newLinkedHashSet());
                for (HintPlanNode rightTable : rightSet) {
                    String tableName = rightTable.getName();
                    Set<HintPlanNode> set = Optional.ofNullable(dependMap.get(tableName)).orElse(Sets.newLinkedHashSet());
                    set.addAll(leftSet);
                    hintPlanNodeMap.put(tableName, Type.AND);
                    dependMap.put(tableName, set);
                }
            } else if (node.getType() == Type.ER) {
                String data = node.getData();
                String[] erSplit = data.split(comma);
                HashSet<HintPlanNode> erSets = Sets.newHashSet();
                for (String table : erSplit) {
                    erSets.add(nodeMap.get(table));
                    erMap.put(table, erSets);
                }
            }
            if (Objects.nonNull(rightNode)) {
                queue.offer(rightNode);
            }
            if (Objects.nonNull(leftNode)) {
                queue.offer(leftNode);
            }

        }
    }

    private Set<HintPlanNode> buildTables(Node node, Set<HintPlanNode> tableSet) {
        if (Objects.isNull(node)) {
            return tableSet;
        }
        if (node.getType() == Type.ER) {
            String[] erSplit = node.getData().split(comma);
            Arrays.stream(erSplit).forEach(table -> tableSet.add(nodeMap.get(table)));
        } else if (!node.isTable()) {
            tableSet.add(HintPlanNode.of(node.getData()));
        }
        buildTables(node.getRightNode(), tableSet);
        buildTables(node.getLeftNode(), tableSet);
        return tableSet;
    }

    private void setLeftNode(Node curNode, Node preNode) {
        curNode.setLeftNode(preNode);
        preNode.setParent(curNode);
    }

    private void setRightNode(Node curNode, Node preNode) {
        curNode.setRightNode(preNode);
        preNode.setParent(curNode);
    }


    public HashMap<String, Set<HintPlanNode>> getDependMap() {
        return dependMap;
    }

    public HashMap<String, Set<HintPlanNode>> getErMap() {
        return erMap;
    }

    public LinkedHashMap<String, Type> getHintPlanNodeMap() {
        return hintPlanNodeMap;
    }

    public enum Type {
        ER, AND, OR, Other
    }

    private class Node {
        String data;
        private Node parent;
        private Node leftNode;
        private Node rightNode;
        private Type type = Type.Other;

        Node(String data) {
            this.data = data;
        }

        Node(String data, Type type) {
            this.data = data;
            this.type = type;
        }

        public boolean isTable() {
            return data.equals(or) || data.equals(and);
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }

        public Node getParent() {
            return parent;
        }

        public void setParent(Node parent) {
            this.parent = parent;
        }

        public Node getLeftNode() {
            return leftNode;
        }

        public void setLeftNode(Node leftNode) {
            this.leftNode = leftNode;
        }

        public Node getRightNode() {
            return rightNode;
        }

        public void setRightNode(Node rightNode) {
            this.rightNode = rightNode;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

    }

}
