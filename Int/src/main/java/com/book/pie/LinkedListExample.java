package com.book.pie;

public class LinkedListExample {

    public static void main(String args[]) {
       CustomLinkedList<String> linkedList = new CustomLinkedList<String>();
       linkedList.add("First Node");
       linkedList.add("Second Node");
       linkedList.add("Third Node");
       linkedList.add(1, "index test Fifth node");
       linkedList.print();

       System.out.println("*********Zeroth index test*********");
       linkedList.add(0, "Oth index test Zero node");
       linkedList.print();

        System.out.println("*********Last index test*********");
        linkedList.add(5, "Last index test");
        linkedList.print();

        System.out.println("*********Last node pointer check*********");
        linkedList.add("SHould be last");
        linkedList.print();

        System.out.println("*********Delete First Node*********");
        linkedList.remove(0);
        linkedList.print();

        System.out.println("*********Delete middle Node*********");
        linkedList.remove(2);
        linkedList.print();

        System.out.println("*********Delete middle Node*********");
        linkedList.remove(4);
        linkedList.print();

        System.out.println("*********get middle Node*********");
        System.out.println(linkedList.get(2));
    }

}

class CustomLinkedList<T> {
    private Node<T> head;
    private Node<T> tail;

    public T get(int index) {
        int counter = 0;
        Node<T> tempNode = head;
        while(tempNode != null) {
            if (counter == index) {
                return tempNode.getData();
            }

            ++counter;
            tempNode = tempNode.getNode();
        }

        return null;
    }

    public void add(T t) {
        if (this.head == null) {
           head = new Node<>();
           head.setData(t);
           tail = head;
        } else {
            Node<T> node = new Node<>();
            node.setData(t);
            tail.setNode(node);
            tail = node;
        }
    }

    public void add(int index, T t) {
        Node<T> node = new Node<>();
        node.setData(t);
        if (index == 0) { // add head, update reference to head
            node.setNode(head);
            head = node;
        } else {
            int counter = 0;
            Node<T> tempNode = head;
            while (tempNode != null) {
                if (counter++ == index - 1) { //For addition to index node, always got the previous node for not to loose reference of next node
                    Node<T> nextNode = tempNode.getNode();
                    if (nextNode == null) //update tail
                        tail = node;
                    tempNode.setNode(node);
                    node.setNode(nextNode);
                    break;
                }
                tempNode = tempNode.getNode();
            }
        }
    }

    public void remove(int index) {
        if (index == 0) {
           head = head.getNode();
        } else {
            int counter = 0;
            Node<T> tempNode = head;
            while (tempNode != null) {
                if (counter++ == index - 1) {
                    Node<T> nextNode = tempNode.getNode();
                    if (nextNode == null) //update tail
                        tail = tempNode;

                    tempNode.setNode(nextNode.getNode());
                    break;
                }
                tempNode = tempNode.getNode();
            }
        }
    }

    public void print() {
        Node<T> tempNode = head;
        while(tempNode != null) {
            System.out.println(tempNode.getData());
            tempNode = tempNode.getNode();
        }
    }
}

class Node<T> {
    private Node<T> node;
    private T data;

    public Node<T> getNode() {
        return node;
    }

    public void setNode(Node<T> node) {
        this.node = node;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}