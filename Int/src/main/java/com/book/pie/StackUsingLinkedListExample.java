package com.book.pie;

public class StackUsingLinkedListExample {

    public static void main(String args[]) {
        StackUsingLinkedList<Integer> stack = new StackUsingLinkedList<>();
        System.out.println("Exception test");
         try { stack.pop(); } catch (RuntimeException e) {
             System.out.println(e.getMessage());
         }
        stack.push(1);
        stack.push(2);
        stack.push(3);
        stack.push(4);
        stack.push(5);
        stack.print();
        System.out.println("Pop");
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        System.out.println(stack.pop());
        System.out.println("Peek");
        System.out.println(stack.peek());
        System.out.println(stack.peek());
    }
}

class StackUsingLinkedList<T> {
    private CustomLinkedList<T> linkedList = new CustomLinkedList<>();

    public T peek() {
         T t = linkedList.get(0);
         if (t == null) {
             throw new RuntimeException("Empty stack exception");
         }

         return t;
    }

    public T pop() {
        T t = linkedList.get(0);
        if (t == null) {
            throw new RuntimeException("Empty stack exception");
        }
        linkedList.remove(0);
        return t;
    }

    public void push(T t) {
        linkedList.add(0, t);
    }

    public void print() {
        linkedList.print();
    }
}
