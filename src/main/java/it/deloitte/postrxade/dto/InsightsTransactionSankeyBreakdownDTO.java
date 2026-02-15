package it.deloitte.postrxade.dto;


import it.deloitte.postrxade.exception.NotFoundRecordException;
import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class InsightsTransactionSankeyBreakdownDTO {
    List<Node> nodesList;
    List<Link> linksList;

    public void addItem(String name, int source, int target, long value)  {

        if(target==-1){
            nodesList = nodesList == null ? new ArrayList<>() : nodesList;
            linksList = linksList == null ? new ArrayList<>() : linksList;
            nodesList.add(new Node(name));
            return;
        }
        nodesList.add(new Node(name));
        linksList.add(new Link(source, target, value));
    }

}

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
class Node{
    public String name;

}

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
class Link{
    public int source;
    public int target;
    public long value;
}