package br.com.programaai.avro;

import br.com.programaai.loja.Endereco;
import br.com.programaai.loja.Pedido;
import br.com.programaai.loja.ProdutoResumo;
import br.com.programaai.loja.TipoEntrega;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class Application {

  private static final List<Pedido> PEDIDOS;

  static {
    final List<Pedido> pedidos = Arrays.asList(
        Pedido.newBuilder()
            .setCodigoPedido(1L)
            .setDataPedido(dateToLong(2019, 10, 2))
            .setTipoEntrega(TipoEntrega.FISICO)
            .setEmailCliente(null)
            .setEnderecoEntrega(Endereco.newBuilder()
                .setLogradouro("Rua Jamil Macedo, 58")
                .setBairro("Jardim Gramado")
                .setCidade("Jurupema")
                .setEstado("GO")
                .setCEP("75212-200")
                .setTelefone("(62) 93295-2201")
                .build())
            .setProdutos(Arrays.asList(
                ProdutoResumo.newBuilder().setCodigo(1L).setNome("Liquidificador")
                    .setPreco("150,99").setDescricao("Belo liquidificador").build(),
                ProdutoResumo.newBuilder().setCodigo(2L).setNome("Refrigerador")
                    .setPreco("1399,99").setDescricao(null).build()))
            .build(),
        Pedido.newBuilder()
            .setCodigoPedido(2L)
            .setDataPedido(dateToLong(2019, 9, 15))
            .setTipoEntrega(TipoEntrega.DIGITAL)
            .setEmailCliente("client1@pedido.com")
            .setEnderecoEntrega(null)
            .setProdutos(Arrays.asList(
                ProdutoResumo.newBuilder().setCodigo(1L).setNome("The Amazin Spiderman")
                    .setPreco("59,99").build(),
                ProdutoResumo.newBuilder().setCodigo(2L).setNome("Assassin's Creed Odissey")
                    .setPreco("199,99").build()))
            .build()
    );

    PEDIDOS = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      PEDIDOS.addAll(pedidos);
    }
  }

  /*
   * Exemplo de uso
   * java -jar <arquivo.jar> read arquivo.avro
   * java -jar <arquivo.jar> write arquivo.avro
   */
  public static void main(String[] args) {
    String arg = args[0];
    String filename = args[1];

    try {
      if (arg.equals("read")) {
        readOrders(filename);
      } else {
        writeOrders(filename);
      }
    } catch (IOException e) {
      print(System.err, "Erro ao processar arquivo: %s", e.getMessage());
      e.printStackTrace(System.err);
      System.exit(-1);
    }
  }

  private static void readOrders(String inFilename) throws IOException {
    DatumReader<Pedido> datumReader = new SpecificDatumReader<>(Pedido.class);

    long t0 = System.nanoTime();
    int counter = 0;
    Pedido sample = null;
    try (DataFileReader<Pedido> fileReader = new DataFileReader<>(new File(inFilename), datumReader)) {
      while (fileReader.hasNext()) {
        sample = fileReader.next();
        if (counter == 0) {
          print("Primeira iteracao em %.8fs", delta(t0));
        }
        counter += 1;
      }
    }

    print("%d registros lidos em %.3fs", counter, delta(t0));
    print("Exemplo de registro:%n%s", sample);
  }


  private static void writeOrders(String outFilename) throws IOException {
    DatumWriter<Pedido> datumWriter = new SpecificDatumWriter<>(Pedido.class);

    long t0 = System.nanoTime();
    try(DataFileWriter<Pedido> dataFileWriter = new DataFileWriter<>(datumWriter)) {
      dataFileWriter.setCodec(CodecFactory.deflateCodec(9));
      dataFileWriter.create(Pedido.SCHEMA$, new File(outFilename));
      for (Pedido pedido : PEDIDOS) {
        dataFileWriter.append(pedido);
      }
    }
    print("%d registros escritos em %.8fms", PEDIDOS.size(), delta(t0) * 1e-6);
  }

  /* FUNCOES UTILITARIAS */

  private static void print(String format, Object... args) {
    print(System.out, format, args);
  }

  private static void print(PrintStream out, String format, Object... args) {
    out.println(String.format(format, args));
  }

  private static double delta(long t0) {
    return (System.nanoTime() - t0) * 1e-9;
  }

  private static long dateToLong(int year, int month, int day) {
    LocalDate d = LocalDate.of(year, month, day);
    return d
        .atStartOfDay()
        .atZone(ZoneId.of("UTC"))
        .toInstant()
        .toEpochMilli();
  }
}
