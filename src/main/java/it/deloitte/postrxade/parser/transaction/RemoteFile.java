package it.deloitte.postrxade.parser.transaction;

import java.io.InputStream;

public record RemoteFile(String name, InputStream stream) {
}
